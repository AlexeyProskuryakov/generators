# coding=utf-8
import logging
import time
from multiprocessing import Process

from wsgi.db import HumanStorage
from wsgi.properties import force_post_manager_sleep_iteration_time
from wsgi.rr_people.posting.posts import PostsStorage, PS_READY
from wsgi.rr_people.posting.youtube_posts import YoutubeChannelsHandler
from wsgi.rr_people.states.processes import ProcessDirector

log = logging.getLogger("posts")

IMPORTANT_POSTS_SUPPLIER_PROCESS_ASPECT = "im_po_su_aspect"


class ImportantYoutubePostSupplier(Process):
    """
    Process which get humans config and retrieve channel_id, after retrieve new posts from it and
    """

    def __init__(self, ms=None, ps=None):
        super(ImportantYoutubePostSupplier, self).__init__()

        self.main_storage = ms or HumanStorage("im po su main")
        self.post_storage = ps or PostsStorage("im po su ph")
        self.posts_supplier = YoutubeChannelsHandler(self.post_storage)

        self.pd = ProcessDirector("im po su")

        log.info("important post supplier started")

    def load_new_posts_for_human(self, human_name, channel_id):
        try:
            new_posts = self.posts_supplier.get_new_channel_videos(channel_id)
            new_posts = filter(lambda x: x.for_sub is not None, new_posts)
            log.info("At youtube for [%s] found [%s] new posts:\n%s" % (
                human_name, len(new_posts), ' youtube \n'.join([str(post) for post in new_posts])))

            for post in new_posts:
                self.post_storage.add_generated_post(post, post.for_sub, human=human_name, important=True,
                                                     state=PS_READY)

            return len(new_posts), None

        except Exception as e:
            log.error("Exception at loading youtube new posts %s, for %s at %s" % (e, human_name, channel_id))
            log.exception(e)
            return e.message, e

    def run(self):
        if not self.pd.can_start_aspect(IMPORTANT_POSTS_SUPPLIER_PROCESS_ASPECT, self.pid).get("started"):
            log.info("important posts supplier instance already work")
            return

        while 1:
            humans_data = self.main_storage.get_humans_info(projection={"user": True, "subs": True, "channel_id": True})
            for human_data in humans_data:
                channel_id = human_data.get("channel_id")
                if channel_id:
                    self.load_new_posts_for_human(human_data.get("user"), channel_id)

            time.sleep(force_post_manager_sleep_iteration_time)


class NoisePostsAutoAdder(Process):
    '''
    Must be init and run server if will setting auto removing generated post to balancer

    1) В глобальных конфигах должно быть установлен конфиг с ключем == имени этого дерьма
    2) Данные этого конфига должны быть on == true и after == количеству секунд после которых
    сгенеренные посты в состоянии PS_READY будут засунуты в балансер и определенны их идентификаторы каналов


    '''
    name = "noise_auto_adder"

    def __init__(self):
        super(NoisePostsAutoAdder, self).__init__()
        self.process_director = ProcessDirector("noise pp")
        self.posts_storage = PostsStorage("noise pp")
        self.main_db = HumanStorage("noise pp")

    def run(self):
        if not self.process_director.can_start_aspect(self.name, self.pid).get("started"):
            log.info("%s instance already work" % self.name)
            return

        while 1:
            cfg = self.main_db.get_global_config(self.name)
            is_on = cfg.get("on")
            if not is_on:
                log.info("in configuration noise posts auto adder is off i go out")
                return

            after = cfg.get("after")
            if not after:
                after = 3600

            counter = 0
            for post in self.posts_storage.posts.find({"time": {"$lt": time.time() - after}}):
                self.posts_storage.set_post_state(post.get("url_hash"), PS_READY)
                counter += 1

            log.info("Auto add to balancer will add %s posts" % counter)
            time.sleep(after / 10)
