from typing import Dict, List, Optional, Any
import httpx
import websockets
import json
import asyncio
import logging
from tenacity import retry, stop_after_attempt, wait_exponential
import base64
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.backends import default_backend
from app.shared.config.settings import CAPITAL_SETTINGS

logger = logging.getLogger(__name__)

class CapitalComAPI:
    """
    Client for interacting with the Capital.com API.
    Handles authentication, session management, and API requests.
    """
    
    def __init__(
        self,
        api_key: Optional[str] = None,
        api_identifier: Optional[str] = None,
        api_password: Optional[str] = None,
        base_url: Optional[str] = None,
        streaming_url: Optional[str] = None,
        is_encrypted_password: bool = True
    ):
        self.api_key = api_key or CAPITAL_SETTINGS.CAPITAL_API_KEY
        self.api_identifier = api_identifier or CAPITAL_SETTINGS.CAPITAL_API_IDENTIFIER
        self.api_password = api_password or CAPITAL_SETTINGS.CAPITAL_API_PASSWORD
        self.base_url = base_url or CAPITAL_SETTINGS.CAPITAL_API_BASE_URL
        self.streaming_url = streaming_url or CAPITAL_SETTINGS.CAPITAL_API_STREAMING_URL
        self.is_encrypted_password = is_encrypted_password
        
        self.cst = None
        self.security_token = None
        self.session_valid_until = None
        self.http_client = httpx.Client(timeout=30.0)
        self.websocket_lock = asyncio.Lock()  # Add a lock for WebSocket access
        
        # Add default headers
        self.http_client.headers.update({
            "Content-Type": "application/json",
            "Accept": "application/json",
        })

    def encrypt_password(self, encryption_data: dict) -> str:
        try:
            # Concatenate password and timestamp
            timestamp = encryption_data.get('timeStamp')
            encryption_key = encryption_data.get('encryptionKey')
            password = self.api_password
            data = f"{password}|{timestamp}"
            
            # Convert to UTF-8 bytes and Base64 encode
            data_bytes = data.encode('utf-8')
            data_base64 = base64.b64encode(data_bytes)
            
            # Decode the Base64 encoded encryption key to get DER bytes
            der_encoded_key = base64.b64decode(encryption_key.encode('utf-8'))
            
            # Load the public key from DER format
            public_key = serialization.load_der_public_key(
                der_encoded_key,
                backend=default_backend()
            )
            
            # Encrypt the Base64 data with RSA PKCS1v1.5 padding
            encrypted_data = public_key.encrypt(
                data_base64,
                padding.PKCS1v15()
            )
            
            # Base64 encode the encrypted result and return as string
            encrypted_base64 = base64.b64encode(encrypted_data)
            return encrypted_base64.decode('utf-8')
        
        except Exception as e:
            raise RuntimeError(f"Encryption failed: {e}") from e
        
    async def create_session(self) -> bool:
        """
        Create a new session with the Capital.com API.
        Returns True if successful, False otherwise.
        """
        try:
            # If using encrypted password, first get encryption key
            if self.is_encrypted_password:
                encryption_data = await self._get_encryption_key()
                encrypted_password = self.encrypt_password(encryption_data)
            else:
                encrypted_password = None
            
            # Create session
            url = f"{self.base_url}/api/v1/session"
            headers = {"X-CAP-API-KEY": self.api_key}
            
            payload = {
                "identifier": self.api_identifier,
                "password": encrypted_password if self.is_encrypted_password else self.api_password,
                "encryptedPassword": self.is_encrypted_password
            }
            
            response = self.http_client.post(url, headers=headers, json=payload)
            
            if response.status_code == 200:
                # Extract session tokens from headers
                self.cst = response.headers.get("CST")
                self.security_token = response.headers.get("X-SECURITY-TOKEN")
                
                # Update client headers with session tokens
                self.http_client.headers.update({
                    "CST": self.cst,
                    "X-SECURITY-TOKEN": self.security_token
                })
                
                # Set session expiry time (10 minutes from now)
                self.session_valid_until = asyncio.get_event_loop().time() + 600
                
                logger.info("Successfully created Capital.com API session")
                return True
            else:
                logger.error(f"Failed to create session: {response.status_code} - {response.text}")
                return False
                
        except Exception as e:
            logger.error(f"Error creating session: {str(e)}")
            return False
    
    async def _get_encryption_key(self) -> Dict:
        """
        Get encryption key for password encryption.
        """
        url = f"{self.base_url}/api/v1/session/encryptionKey"
        headers = {"X-CAP-API-KEY": self.api_key}
        
        response = self.http_client.get(url, headers=headers)
        
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Failed to get encryption key: {response.status_code} - {response.text}")
    
    async def ensure_session_valid(self) -> bool:
        """
        Ensure that the current session is valid, creating a new one if necessary.
        """
        current_time = asyncio.get_event_loop().time()
        
        # If session is not valid or about to expire, create a new one
        if (not self.cst or not self.security_token or 
            not self.session_valid_until or 
            current_time > self.session_valid_until - 60):  # Renew 1 minute before expiry
            
            return await self.create_session()
        
        return True
    
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=10))
    async def get(self, endpoint: str, params: Optional[Dict] = None) -> Dict:
        """
        Make a GET request to the Capital.com API.
        """
        await self.ensure_session_valid()
        
        url = f"{self.base_url}{endpoint}"
        response = self.http_client.get(url, params=params)
        
        if response.status_code == 200:
            return response.json()
        else:
            logger.error(f"GET request failed: {response.status_code} - {response.text}")
            response.raise_for_status()
    
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=10))
    async def post(self, endpoint: str, data: Dict) -> Dict:
        """
        Make a POST request to the Capital.com API.
        """
        await self.ensure_session_valid()
        
        url = f"{self.base_url}{endpoint}"
        response = self.http_client.post(url, json=data)
        
        if response.status_code in (200, 201):
            return response.json()
        else:
            logger.error(f"POST request failed: {response.status_code} - {response.text}")
            response.raise_for_status()
    
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=10))
    async def put(self, endpoint: str, data: Dict) -> Dict:
        """
        Make a PUT request to the Capital.com API.
        """
        await self.ensure_session_valid()
        
        url = f"{self.base_url}{endpoint}"
        response = self.http_client.put(url, json=data)
        
        if response.status_code == 200:
            return response.json()
        else:
            logger.error(f"PUT request failed: {response.status_code} - {response.text}")
            response.raise_for_status()
    
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=10))
    async def delete(self, endpoint: str) -> Dict:
        """
        Make a DELETE request to the Capital.com API.
        """
        await self.ensure_session_valid()
        
        url = f"{self.base_url}{endpoint}"
        response = self.http_client.delete(url)
        
        if response.status_code == 200:
            return response.json()
        else:
            logger.error(f"DELETE request failed: {response.status_code} - {response.text}")
            response.raise_for_status()
    
    async def connect_websocket(self) -> websockets.WebSocketClientProtocol:
        """
        Connect to the Capital.com WebSocket API.
        """
        await self.ensure_session_valid()
        
        websocket = await websockets.connect(self.streaming_url)
        
        # Send authentication message
        auth_message = {
            "destination": "marketData.subscribe",
            "correlationId": "1",
            "cst": self.cst,
            "securityToken": self.security_token,
            "payload": {
                "epics": CAPITAL_SETTINGS.INSTRUMENTS,
            }
        }
        
        async with self.websocket_lock:  # Use the lock for WebSocket operations
            await websocket.send(json.dumps(auth_message))
            response = await websocket.recv()
            response_data = json.loads(response)
        
        if response_data.get("status") != "OK":
            raise Exception(f"WebSocket authentication failed: {response}")
        
        logger.info("Successfully connected to Capital.com WebSocket API")
        return websocket
    
    async def subscribe_to_market_data(self, websocket: websockets.WebSocketClientProtocol, epics: List[str]) -> None:
        """
        Subscribe to market data for the specified epics.
        """
        subscription_message = {
            "destination": "marketData.subscribe",
            "correlationId": "2",
            "cst": self.cst,
            "securityToken": self.security_token,
            "payload": {
                "epics": epics
            }
        }
        
        async with self.websocket_lock:  # Use the lock for WebSocket operations
            await websocket.send(json.dumps(subscription_message))
            response = await websocket.recv()
            response_data = json.loads(response)
        
        if response_data.get("status") != "OK":
            raise Exception(f"Market data subscription failed: {response}")
        
        logger.info(f"Successfully subscribed to market data for epics: {epics}")
    
    async def ping_websocket(self, websocket: websockets.WebSocketClientProtocol) -> None:
        """
        Ping the WebSocket to keep the connection alive.
        """
        ping_message = {
            "destination": "ping",
            "correlationId": "3"
        }
        
        async with self.websocket_lock:  # Use the lock for WebSocket operations
            await websocket.send(json.dumps(ping_message))
            response = await websocket.recv()
            response_data = json.loads(response)
        
        if response_data.get("status") != "OK":
            raise Exception(f"WebSocket ping failed: {response}")
    
    async def get_markets(self, search_term: Optional[str] = None) -> Dict:
        """
        Get available markets, optionally filtered by search term.
        """
        params = {}
        if search_term:
            params["searchTerm"] = search_term
        
        return await self.get("/api/v1/markets", params)
    
    async def get_positions(self) -> Dict:
        """
        Get all open positions.
        """
        return await self.get("/api/v1/positions")
    
    async def create_position(
        self,
        epic: str,
        direction: str,
        size: float,
        guaranteed_stop: bool = False,
        stop_level: Optional[float] = None,
        profit_level: Optional[float] = None,
        trailing_stop: bool = False,
        stop_distance: Optional[float] = None
    ) -> Dict:
        """
        Create a new position.
        """
        data = {
            "epic": epic,
            "direction": direction,
            "size": size,
            "guaranteedStop": guaranteed_stop
        }
        
        if guaranteed_stop and stop_level is not None:
            data["stopLevel"] = stop_level
        
        if profit_level is not None:
            data["profitLevel"] = profit_level
        
        if trailing_stop:
            data["trailingStop"] = True
            
            if stop_distance is not None:
                data["stopDistance"] = stop_distance
        
        return await self.post("/api/v1/positions", data)
    
    async def close_position(self, deal_id: str) -> Dict:
        """
        Close an open position.
        """
        data = {
            "dealId": deal_id
        }
        
        return await self.delete(f"/api/v1/positions/{deal_id}")
    
    async def get_price_history(
        self,
        epic: str,
        resolution: str,
        from_date: Optional[str] = None,
        to_date: Optional[str] = None,
        max_bars: Optional[int] = None
    ) -> Dict:
        """
        Get price history for a specific market.
        """
        params = {
            "resolution": resolution
        }
        
        if from_date:
            params["from"] = from_date
        
        if to_date:
            params["to"] = to_date
        
        params["max"] = max_bars or 1000
        
        return await self.get(f"/api/v1/prices/{epic}", params)

    # Newly added instrument & utility methods
    async def list_instruments(
        self,
        epics: Optional[List[str]] = None,
        search_term: Optional[str] = None
    ) -> Dict:
        """Retrieve specific instruments by epic list or search term."""
        params: Dict[str, Any] = {}
        if epics:
            params["epics"] = ",".join(epics)
        if search_term:
            params["searchTerm"] = search_term
        return await self.get("/api/v1/markets", params)