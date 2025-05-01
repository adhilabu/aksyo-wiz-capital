import requests

class TelegramAPI:
    def __init__(self, bot_token):
        self.base_url = f"https://api.telegram.org/bot{bot_token}/"

    def send_message(self, chat_id, text):
        url = self.base_url + "sendMessage"
        payload = {
            "chat_id": chat_id,
            "text": text
        }
        response = requests.post(url, json=payload)
        return response.json()

    def set_webhook(self, url):
        webhook_url = self.base_url + "setWebhook"
        payload = {
            "url": url
        }
        response = requests.post(webhook_url, json=payload)
        return response.json()

    def save_user_data(self, user_data):
        # Implement your logic to save user data
        pass
