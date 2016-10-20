import redis
from multiprocessing import Process

from rr_lib.cm import ConfigManager
from wsgi.youtube.store import YMStorage

to_engie = "TO_ENGINE"
to_result = lambda x: "TO_RESULT_%s" % x


class ExperimentDataBroker():
    def __init__(self):
        cm = ConfigManager()
        self.redis = redis.StrictRedis(host=cm.get("ym_redis_address"),
                                       port=int(cm.get("ym_redis_port")),
                                       password=cm.get("ym_redis_password"))

        self.ym_store = YMStorage()

    def get_experiments(self):
        pubsub = self.redis.pubsub()
        pubsub.subscribe(to_engie)
        for message in pubsub.listen(ignore_subscribe_messages=True):
            exp_id = message.get("data")
            exp_data = self.ym_store.get_experiment_data(exp_id)
            yield exp_data

    def new_experiment(self, keywords, m_keywords, c_filter, v_filter, time_to_see, agg_filter):
        exp_id, _ = self.ym_store.new_experiment(keywords, m_keywords, c_filter, v_filter, time_to_see,
                                                 agg_filter)
        self.redis.publish(to_engie, exp_id)

    def publish_experiment_result(self, exp_id, result_id, data):
        self.ym_store.add_result(exp_id, result_id, data)
        self.redis.lpush(to_result(exp_id), result_id)

    def get_experiment_results(self, exp_id):
        while 1:
            result_id = self.redis.rpop(to_result(exp_id))
            if not result_id:
                break
            data = self.ym_store.pop_result(exp_id, result_id)
            yield data



class ExperimentsProcess(Process):
    pass