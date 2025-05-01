import json
import redis

class RedisCache:
    def __init__(self, host='localhost', port=6379, db=0):
        """
        Initialize the RedisCache object.
        """
        self.client = redis.StrictRedis(host=host, port=port, db=db, decode_responses=True)

    def set_key(self, key, value, ttl=None):
        """
        Set a key-value pair in Redis with an optional TTL.
        """
        value_json = json.dumps(value)
        self.client.set(key, value_json, ex=ttl)

    def key_exists(self, key):
        """
        Check if a key exists in Redis.
        """
        return self.client.exists(key)

    def delete_key(self, key):
        """
        Delete a key from Redis.
        """
        self.client.delete(key)

    def get_key(self, key):
        """
        Retrieve a value from Redis by its key.
        """
        value_json = self.client.get(key)
        return json.loads(value_json) if value_json else None