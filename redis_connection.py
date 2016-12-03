#!/usr/bin/env python

import redis


class RedisConnection(object):
    def __init__(self):
        self.redis_client = redis.StrictRedis(host='localhost', port=6379)

    def write_data(self, key, value):
        self.redis_client.set(key, value)

    def print_info(self):
        self.redis_client.info()
