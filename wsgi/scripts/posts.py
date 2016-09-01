import random

from wsgi.db import HumanStorage
from wsgi.rr_people.posting.posts import PostsStorage, PS_READY


def fill_generated_posts_by_humans():
    hs = HumanStorage(name="script")
    ps = PostsStorage(hs=hs)
    for post in ps.posts.find({"human": {"$exists": False}, "state": PS_READY}):
        result = ps.posts.update_one(post, {"$set": {"human": random.choice(hs.get_humans_of_sub(post.get("sub")))}})
        print result


if __name__ == '__main__':
    fill_generated_posts_by_humans()
