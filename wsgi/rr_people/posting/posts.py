import json

import time

from wsgi.db import DBHandler

PS_PREPARED = "prepared"
PS_READY = "ready"
PS_POSTED = "posted"
PS_AT_QUEUE = "at_queue"

PS_BAD = "bad"

PS_NO_POSTS = "no_posts"
PS_ERROR = "error"


def URL_HASH(url):
    return str(hash(url))


class PostSource(object):
    @staticmethod
    def deserialize(raw_data):
        data = json.loads(raw_data)
        return PostSource.from_dict(data)

    @staticmethod
    def from_dict(data):
        ps = PostSource(data.get("url"),
                        data.get("title"),
                        data.get("for_sub"),
                        data.get("at_time"),
                        data.get("url_hash"),
                        data.get("important")
                        )
        return ps

    def __init__(self, url, title, for_sub=None, at_time=None, url_hash=None, important=False):
        self.url = url
        self.title = title
        self.for_sub = for_sub
        self.at_time = at_time
        self.url_hash = url_hash or URL_HASH(url)
        self.important = important

    def serialize(self):
        return json.dumps(self.__dict__)

    def to_dict(self):
        return self.__dict__

    def __repr__(self):
        result = "url: [%s] title: [%s] url_hash: [%s]" % (self.url, self.title, self.url_hash)
        if self.for_sub:
            result = "%sfor sub: [%s] " % (result, self.for_sub)
        if self.at_time:
            result = "%stime: [%s]" % (result, self.at_time)
        return result


class PostsStorage(DBHandler):
    def __init__(self, name="?"):
        super(PostsStorage, self).__init__(name=name)
        collection_names = self.db.collection_names(include_system_collections=False)
        if "generated_posts" not in collection_names:
            self.posts = self.db.create_collection("generated_posts")
            self.posts.create_index("url_hash", unique=True)
            self.posts.create_index("sub")
            self.posts.create_index("state")
            self.posts.create_index("time")
        else:
            self.posts = self.db.get_collection("generated_posts")

    # posts
    def set_post_state(self, url_hash, state):
        return self.posts.update_one({"url_hash": url_hash}, {"$set": {"state": state}})

    def get_post_state(self, url_hash):
        found = self.posts.find_one({"url_hash": url_hash}, projection={"state": 1})
        if found:
            return found.get("state")

    def get_posts_with_state(self, state, projection=None, sort=None):
        q = {"state": state}
        proj = projection or {"_id": False}
        cur = self.posts.find(q, projection=proj)
        if sort:
            cur.sort(sort)
        return list(cur)

    def get_post(self, url_hash, projection=None):
        _projection = projection or {"_id": False}
        found = self.posts.find_one({"url_hash": url_hash}, projection=_projection)
        if found:
            return PostSource.from_dict(found), found
        return None, None

    def add_generated_post(self, post, sub, important=False, human=None, state=PS_PREPARED):
        if isinstance(post, PostSource):
            found, _ = self.get_post(post.url_hash, projection={"_id": True})
            if not found:
                data = post.to_dict()
                data['state'] = state
                data['sub'] = sub
                data['time'] = time.time()

                if important:
                    data['important'] = important
                if human:
                    data["human"] = human
                return self.posts.insert_one(data)

    def get_posts_for_sub_with_state(self, sub, state=PS_PREPARED):
        return map(lambda x: PostSource.from_dict(x), self.posts.find({"sub": sub, "state": state}))

    def move_posts_to_ready_state(self, sub):
        self.posts.update_many({"sub": sub, "state": PS_PREPARED}, {"$set": {"state": PS_READY}})

    def remove_posts_of_sub(self, subname):
        result = self.posts.delete_many({"sub": subname})
        return result


if __name__ == '__main__':
    ps = PostSource("http://foo.bar.baz?k=100500&w=qwerty&tt=ttrtt", "Foo{bar}Baz", "someSub", 100500600)
    raw = ps.serialize()
    print raw
    ps1 = PostSource.deserialize(raw)
    assert ps.at_time == ps1.at_time
    assert ps.title == ps1.title
    assert ps.url == ps1.url
    assert ps.for_sub == ps1.for_sub
