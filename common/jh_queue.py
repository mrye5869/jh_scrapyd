from abc import ABC, abstractmethod
import time
import redis
import logging
from jh_scrapyd.common.utils import (
    str_decode,
    data_encode,
    data_decode
)


def _gen_key(arr, sep: str = ':') -> str:
    """Generate storage key"""
    return sep.join([str_decode(s) for s in arr])


class Queue(ABC):
    # queue type
    SET_NAME = 'queue_set'
    DATA_NAME = 'queue_data'
    # Queue group name (Unified queue name)
    UNIFIED_QUEUE_NAME = 'default'


    def __init__(self, storage, table: str = 'default', is_unified_queue: bool = False):
        """Initialize queue"""
        self.storage = storage
        self.table = table
        self.is_unified_queue = is_unified_queue
        self.now_time = int(time.time())

    @abstractmethod
    def put(self, z_key: str, key: str, data: dict, score: float = 1) -> bool:
        """Writes data with fractions"""

    @abstractmethod
    def pop(self, z_key: str, desc: bool = True):
        """Sort the columns as specified"""

    @abstractmethod
    def remove(self, z_key: str, key: str) -> bool:
        """Delete data from the queue"""

    @abstractmethod
    def list(self, z_key: str, desc: bool = True):
        """Get all queue data in order"""

    @abstractmethod
    def count(self, z_key: str) -> int:
        """Get number of queues"""

    @abstractmethod
    def clear(self, z_key: str):
        """Clear the specified collection data"""

    @abstractmethod
    def queues(self) -> list:
        """Get all queue names"""

    def score_weight(self, score: float = 1) -> int:
        """Calculate score weight"""
        return int(self.now_time * score)

    def _gen_data_key(self, z_key, key) -> str:
        if self.is_unified_queue:
            # Unified queue
            z_key = self.UNIFIED_QUEUE_NAME
        # Construct a key array
        key_arr = [self.table, self.DATA_NAME, z_key]
        if key:
            key_arr.append(key)
        return _gen_key(key_arr)

    def _gen_set_key(self, z_key) -> str:
        if self.is_unified_queue:
            # Unified queue
            z_key = self.UNIFIED_QUEUE_NAME
        # Construct a key array
        key_arr = [self.table, self.SET_NAME, z_key]
        return _gen_key(key_arr)


class RedisQueue(Queue):
    def put(self, z_key: str, key: str, data: dict, score: float = 1) -> bool:
        if not data:
            return False
        LUA_SCRIPT = """
            redis.call('ZADD', KEYS[1], ARGV[1], ARGV[2])
            redis.call('SET', KEYS[2], ARGV[3])
            return true
        """
        try:
            script = self.storage.register_script(LUA_SCRIPT)
            result = script(keys=[self._gen_set_key(z_key), self._gen_data_key(z_key, key)],
                            args=[self.score_weight(score), key, data_encode(data)])
            return True if result else False
        except redis.exceptions.RedisError as e:
            logging.error(f"Error adding task to queue: {e}")
            return False

    def pop(self, z_key: str, desc: bool = True):
        LUA_POP_SCRIPT = """
            local set_key = KEYS[1]
            local direction = ARGV[1]
            local basis_data_key = ARGV[2]

            local value
            if direction == "desc" then
                value = redis.call('zpopmax', set_key, 1)
            else
                value = redis.call('zpopmin', set_key, 1)
            end

            if not value or #value == 0 or not value[1] or #value[1] == 0 then
                return nil
            end

            local key = value[1]
            if not key then
                return nil
            end

            local data_key = basis_data_key .. ":" .. key
            local result = redis.call('get', data_key)

            redis.call('del', data_key)

            return {key, result}
        """
        try:
            result = self.storage.eval(
                LUA_POP_SCRIPT,
                # This represents the number of elements in the KEYS array
                1,
                self._gen_set_key(z_key),
                "desc" if desc else "asc",
                # Pass in z_key to dynamically generate data_key
                self._gen_data_key(z_key, "")
            )
            if not result:
                return None
            member, data = result
            return data_decode(data)
        except redis.RedisError as e:
            logging.error(f"Error popping task from queue: {e}")
            return None

    def remove(self, z_key: str, key: str) -> bool:
        LUA_REMOVE_SCRIPT = """
        redis.call('ZREM', KEYS[1], ARGV[1])
        redis.call('DEL', KEYS[2])
        return true
        """
        try:
            script = self.storage.register_script(LUA_REMOVE_SCRIPT)
            result = script(keys=[self._gen_set_key(z_key), self._gen_data_key(z_key, key)],
                            args=[key])
            return result == 1
        except redis.exceptions.RedisError as e:
            logging.error(f"Error removing task from queue: {e}")
            return False

    def list(self, z_key: str, desc: bool = True, count: int = -1):
        try:
            z_key_str = self._gen_set_key(z_key)
            key_list = self.storage.zrange(z_key_str, 0, count, desc, True)
            hash_keys = [self._gen_data_key(z_key, item[0]) for item in key_list]
            values = self.storage.mget(hash_keys)
            return [data_decode(value) for value in values if data_decode(value) is not None]
        except redis.RedisError as e:
            logging.error(f"Error listing tasks from queue: {e}")
            return []

    def count(self, z_key: str) -> int:
        try:
            return self.storage.zcard(self._gen_set_key(z_key))
        except redis.RedisError as e:
            logging.error(f"Error obtaining the number of queues: {e}")
            return 0

    def clear(self, z_key: str):
        try:
            self._clear_by_prefix(self._gen_set_key(z_key))
            self._clear_by_prefix(self._gen_data_key(z_key, ''))
        except redis.RedisError as e:
            logging.error(f"Error clearing queue: {e}")

    def queues(self) -> list:
        # Build the base key for the queue
        base_key = _gen_key([self.table, self.SET_NAME])
        # Generate the full key pattern with wildcard
        key_pattern = _gen_key([base_key, '*'])
        # Retrieve all keys matching the pattern
        keys_ret = self.storage.keys(key_pattern)

        # Use list comprehension to process keys in a concise way
        queues = [
            str_decode(key).replace(_gen_key([base_key, '']), '')
            for key in keys_ret
        ]

        return queues

    def pop_by_set(self, z_key: str, desc: bool = True):
        return self.storage.zpopmax(self._gen_set_key(z_key)) \
            if desc else self.storage.zpopmin(self._gen_set_key(z_key))

    def _clear_by_prefix(self, prefix: str) -> bool:
        try:
            cursor = 0
            while True:
                cursor, keys = self.storage.scan(cursor=cursor, match=f"{prefix}*", count=1000)
                if keys:
                    self.storage.delete(*keys)
                if cursor == 0:
                    break
            return True
        except redis.RedisError as e:
            logging.error(f"Error deleting specified key: {prefix}，error:{e}")
            return False

    def retry_failed_task(self, z_key: str, key: str, data: dict, score: float = 1, max_retries: int = 3):
        retry_count = data.get('retry_count', 0)
        if retry_count < max_retries:
            data['retry_count'] = retry_count + 1
            self.put(z_key, key, data, score)
        else:
            logging.error(f"Max retries reached for task {key}")




if __name__ == '__main__':
    redis_client = redis.StrictRedis(
        host='127.0.0.1',
        port=6379,
        db=0
    )
    test_queue = RedisQueue(redis_client, is_unified_queue=True)

    z_keys = 'test'
    keystr = 'abc'
    data1 = {'params': 'eyJzY3JhcHlfc3ViX3Rhc2tfaWQiOiAiNjY4NzkyYTUzZGNkMWI5NDIwMDU1MjdlIiwgInNjcmFweV90YXNrX2lkIjogIjY2ODc5MmE1M2RjZDFiOTQyMDA1NTI3ZCIsICJzaWduIjogImQxMWFhZjY2NDFlOGYyYzk3YTIxM2I0YTFjYTIwNDFiIiwgInByb2plY3QiOiAiYWR2ZXJ0aXNpbmciLCAic3BpZGVyIjogInNlYXJjaCJ9', 'settings': {}, '_job': '155c5781555011ef8e0808bfb89d1deb', 'name': 'baidu'}

    res = test_queue.put(z_keys, keystr, data1)
    # res = test_queue.list(z_keys)
    # res = test_queue.pop(z_keys, False)
    # res = test_queue.remove(z_keys, keystr)
    # res = test_queue.count(z_keys)
    # res = test_queue.queues()

    print(res)
