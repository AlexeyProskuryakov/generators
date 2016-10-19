import time

from wsgi import ConfigManager
from wsgi.db import DBHandler

CHANNEL_ADDLED_TIME = 60 * 60
VIDEO_ADDLED_TIME = 60 * 10
DEFAULT_TIME_TO_SEE = 3600 * 24 * 7


class YMStorage(DBHandler):
    filter_c_n = "ym_filters"
    channels_c_n = "ym_channels"
    videos_c_n = "ym_videos"
    results_c_n = "exp_results"

    def __init__(self):
        cm = ConfigManager()
        super(YMStorage, self).__init__(name="YM", uri=cm.get("ym_mongo_uri"), db_name=cm.get("ym_db_name"))
        if self.filter_c_n not in self.collection_names:
            self.ym_experiments = self.db.create_collection(self.filter_c_n)
            self.ym_experiments.create_index("exp_id", unque=True)
        else:
            self.ym_experiments = self.db.get_collection(self.filter_c_n)

        if self.channels_c_n not in self.collection_names:
            self.ym_channels = self.db.create_collection(self.channels_c_n, capped=True, size=1024 * 1024)
            self.ym_channels.create_index("channel_id", unique=True)
        else:
            self.ym_channels = self.db.get_collection(self.channels_c_n)

        if self.videos_c_n not in self.collection_names:
            self.ym_videos = self.db.create_collection(self.videos_c_n, capped=True, size=1024 * 1024 * 100)
            self.ym_videos.create_index("video_id", unique=True)
        else:
            self.ym_videos = self.db.get_collection(self.videos_c_n)

        if self.results_c_n in self.collection_names:
            self.results = self.db.create_collection(self.results_c_n)
            self.results.create_index([("result_id", 1), ("exp_id", 1)], unique=True)
        else:
            self.results = self.db.get_collection(self.results_c_n)

    def _check_ids(self, col, ids, id_name):
        found = col.find({id_name: {"$in": ids}})
        found = dict(map(lambda x: (x.get(id_name), x),
                         filter(lambda x: (time.time() - x.get("toggle_time")) > CHANNEL_ADDLED_TIME,
                                found)))

        not_found_ids = set(ids).difference(found.keys())
        found_data = map(lambda x: x[1], filter(lambda x: x[0] not in not_found_ids, found.items()))
        return found_data, not_found_ids

    def new_experiment(self, keywords, minus_keywords, channel_filter, video_filter, time_to_see, aggregate_filter):
        filter_store = {
            "time_to_see": time_to_see,

            "keywords": keywords,
            "m_keywords": minus_keywords,

            "c_filter": channel_filter,
            "v_filter": video_filter,

            "a_filter": aggregate_filter,
        }
        exp_id = str(hash("".join([keywords] + minus_keywords)))
        self.ym_experiments.update_one({"exp_id": exp_id}, {"$set": filter_store}, upsert=True)
        return exp_id, filter_store

    def get_experiment_data(self, exp_id):
        return self.ym_experiments.find_one({"exp_id": exp_id})

    def update_channel(self, channel_id, channel_data, exp_id):
        to_set = dict({"toggle_time": time.time(), "channel_id": channel_id}, **channel_data)
        self.ym_channels.update_one({"channel_id": channel_id}, {"$set": to_set}, upsert=True)
        self.add_channel_to_experiment(exp_id, channel_id)

    def add_channel_to_experiment(self, exp_id, channel_id):
        self.ym_experiments.update_one({"exp_id": exp_id}, {"$addToSet": {"channels": channel_id}})

    def get_addled_channels(self, channel_ids):
        return self._check_ids(self.ym_channels, channel_ids, "channel_id")

    def update_video(self, video_id, video_data):
        to_set = dict({"toggle_time": time.time(), "video_id": video_id}, **video_data)
        self.ym_videos.update_one({"video_id": video_id}, {"$set": to_set}, upsert=True)

    def get_addled_videos(self, video_ids):
        return self._check_ids(self.ym_videos, video_ids, "video_id")

    def add_result(self, exp_id, result_id, data):
        self.results.update_one({"exp_id": exp_id, "result_id": result_id},
                                {"$set": dict({"a_time": time.time()}, **data)})

    def pop_result(self, exp_id, result_id):
        return self.results.find_and_modify({"exp_id": exp_id, "result_id": result_id}, remove=True)
