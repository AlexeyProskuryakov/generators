from wsgi.db import HumanStorage
from wsgi.rr_people.posting.posts import PostsStorage, PS_READY


def clear_posts():
    ps = PostsStorage()
    ps.posts.delete_many({"important": False})



def clear_important_posts():
    ps = PostsStorage()


    for post in ps.posts.find({"important": True}):
        ps.posts.delete_one(post)
        print "delete: ", post


def remove_human_log():
    main = HumanStorage()
    main.human_log.drop()


if __name__ == '__main__':
    pass
    # clear_posts()
    # clear_important_posts()
    # remove_head_noise_from_queue_to_balanser("Shlak2k16")
    # clear_batches("Shlak2k16")
    # remove_human_log()
