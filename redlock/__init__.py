import logging
import textwrap
import time
import uuid
from collections import namedtuple

import redis
from redis.exceptions import RedisError

# Python 3 compatibility
string_type = getattr(__builtins__, 'basestring', str)

try:
    basestring
except NameError:
    basestring = str


Lock = namedtuple("Lock", ("validity", "resource", "key"))


class CannotObtainLock(Exception):
    pass


class MultipleRedlockException(Exception):
    def __init__(self, errors, *args, **kwargs):
        super(MultipleRedlockException, self).__init__(*args, **kwargs)
        self.errors = errors

    def __str__(self):
        return ' :: '.join([str(e) for e in self.errors])

    def __repr__(self):
        return self.__str__()


def get_unique_id():
    return uuid.uuid4().hex


class Redlock(object):
    clock_drift_factor = 0.01
    lock_script = textwrap.dedent("""\
    local current_value = redis.call("get",KEYS[1])
    if current_value == ARGV[1] or current_value == false then
        return redis.call("set",KEYS[1],ARGV[1],"PX",ARGV[2])
    else
        return 0
    end""")
    unlock_script = textwrap.dedent("""\
    if redis.call("get",KEYS[1]) == ARGV[1] then
        return redis.call("del",KEYS[1])
    else
        return false
    end""")

    def __init__(self, connection_list):
        self.servers = []
        for connection_info in connection_list:
            try:
                if isinstance(connection_info, string_type):
                    server = redis.StrictRedis.from_url(connection_info)
                elif type(connection_info) == dict:
                    server = redis.StrictRedis(**connection_info)
                else:
                    server = connection_info
                self.servers.append(server)
            except Exception as e:
                raise Warning(str(e))
        self.quorum = (len(connection_list) // 2) + 1

        if len(self.servers) < self.quorum:
            raise CannotObtainLock(
                "Failed to connect to the majority of redis servers")

    def lock_instance(self, server, resource, val, ttl, force):
        try:
            assert isinstance(ttl, int), 'ttl {} is not an integer'.format(ttl)
        except AssertionError as e:
            raise ValueError(str(e))
        if force:
            print("val", val)
            return server.set(resource, val, px=ttl)
        else:
            return server.eval(self.lock_script, 1, resource, val, ttl)

    def unlock_instance(self, server, resource, val):
        try:
            result = server.eval(self.unlock_script, 1, resource, val)
        except Exception:
            logging.exception("Error unlocking resource %s in server %s", resource, str(server))

        return result

    def lock(self, resource, value, ttl, force=False):
        # Add 2 milliseconds to the drift to account for Redis expires
        # precision, which is 1 millisecond, plus 1 millisecond min
        # drift for small TTLs.
        drift = int(ttl * self.clock_drift_factor) + 2

        redis_errors = list()
        n = 0
        start_time = int(time.time() * 1000)

        del redis_errors[:]
        for server in self.servers:
            try:
                if self.lock_instance(server, resource, value, ttl, force):
                    n += 1
            except RedisError as e:
                redis_errors.append(e)
        elapsed_time = int(time.time() * 1000) - start_time

        validity = int(ttl - elapsed_time - drift)

        if validity > 0 and n >= self.quorum:
            return Lock(validity, resource, value)
        else:
            for server in self.servers:
                try:
                    self.unlock_instance(server, resource, value)
                except:
                    pass
            raise MultipleRedlockException(redis_errors)

    def unlock(self, lock):
        redis_errors = []
        n = 0
        for server in self.servers:
            try:
                result = self.unlock_instance(server, lock.resource, lock.key)
            except RedisError as e:
                redis_errors.append(e)
            else:
                if result:
                    n += 1
        if n >= self.quorum:
            return Lock(0, lock.resource, lock.key)
        else:
            raise MultipleRedlockException(redis_errors)
