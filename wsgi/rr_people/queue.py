import logging

import redis

log = logging.getLogger("queue")


class RedisHandler(object):
    def __init__(self, name="?", clear=False, host=None, port=None, pwd=None, db=None):
        self.redis = redis.StrictRedis(host=host,
                                       port=port,
                                       password=pwd,
                                       db=db or 0
                                       )
        if clear:
            self.redis.flushdb()

        log.info("Production Queue inited for [%s]" % name)
