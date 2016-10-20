# coding=utf-8
import json
from multiprocessing import Process
import os
import re
import time
import sys

from flask import Flask, logging, request, render_template, session, url_for, g, flash
from flask.json import jsonify
from flask_login import LoginManager, login_required
from werkzeug.utils import redirect

from wsgi import tst_to_dt, array_to_string
from wsgi.db import HumanStorage
from wsgi.rr_people import S_WORK, S_SUSPEND, S_STOP, S_END
from wsgi.rr_people.posting import POST_GENERATOR_OBJECTS
from wsgi.rr_people.posting.copy_gen import SubredditsRelationsStore
from wsgi.rr_people.posting.posts import PS_BAD, PostsStorage, PS_PREPARED
from wsgi.rr_people.posting.posts_generator import PostsGenerator
from wsgi.rr_people.posting.posts_important import IMPORTANT_POSTS_SUPPLIER_PROCESS_ASPECT, ImportantYoutubePostSupplier

from wake_up.views import wake_up_app
from rr_lib.users.views import users_app, usersHandler
from states.processes import ProcessDirector

__author__ = '4ikist'

reload(sys)
sys.setdefaultencoding('utf-8')

cur_dir = os.path.dirname(__file__)

app = Flask("Humans", template_folder=cur_dir + "/templates", static_folder=cur_dir + "/static")

app.secret_key = 'foo bar baz'
app.config['SESSION_TYPE'] = 'filesystem'

app.register_blueprint(wake_up_app, url_prefix="/wake_up")
app.register_blueprint(users_app, url_prefix="/u")

app.jinja_env.filters["tst_to_dt"] = tst_to_dt
app.jinja_env.globals.update(array_to_string=array_to_string)

login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'login'


@app.before_request
def load_user():
    if session.get("user_id"):
        user = usersHandler.get_by_id(session.get("user_id"))
    else:
        user = usersHandler.get_guest()
    g.user = user


@login_manager.user_loader
def load_user(userid):
    return usersHandler.get_by_id(userid)


@login_manager.unauthorized_handler
def unauthorized_callback():
    return redirect(url_for('users_api.login'))


@app.route("/")
@login_required
def main():
    user = g.user
    return render_template("main.html", **{"username": user.name})


log = logging.getLogger("web")

db = HumanStorage(name="hs server")

REDIRECT_URI = "http://rr-alexeyp.rhcloud.com/authorize_callback"
C_ID = None
C_SECRET = None

splitter = re.compile('[^\w\d_-]*')

srs = SubredditsRelationsStore("server")
posts_storage = PostsStorage("server", hs=db)
posts_generator = PostsGenerator()


@app.route("/posts")
@login_required
def posts():
    subs = db.get_subs_of_all_humans()
    qp_s = {}
    subs_states = {}
    for sub in subs:
        qp_s[sub] = posts_storage.get_posts_for_sub_with_state(sub, state=PS_PREPARED)
        subs_states[sub] = posts_generator.states_handler.get_posts_generator_state(sub) or S_STOP

    human_names = map(lambda x: x.get("user"), db.get_humans_info(projection={"user": True}))

    stat = posts_storage.posts.aggregate([{"$group": {"_id": "$state", "count": {"$sum": 1}}}])

    return render_template("posts.html", **{"subs": subs_states, "qp_s": qp_s, "humans": human_names, "stat": stat})


@app.route("/generators", methods=["GET", "POST"])
@login_required
def gens_manage():
    if request.method == "POST":
        sub = request.form.get("sub")
        generators = request.form.getlist("gens[]")
        related_subs = request.form.get("related-subs")
        key_words = request.form.get("key-words")

        related_subs = splitter.split(related_subs)
        key_words = splitter.split(key_words)

        srs.add_sub_relations(sub, related_subs)
        posts_generator.generators_storage.set_sub_gen_info(sub, generators, key_words)

        flash(u"Генераторъ постановленъ!")
    gens = POST_GENERATOR_OBJECTS.keys()
    subs = db.get_subs_of_all_humans()
    return render_template("generators.html", **{"subs": subs, "gens": gens})


@app.route("/generators/sub_info", methods=["POST"])
@login_required
def sub_gens_cfg():
    data = json.loads(request.data)
    sub = data.get("sub")
    related = srs.get_related_subs(sub)
    generators = posts_generator.generators_storage.get_sub_gen_info(sub)

    return jsonify(**{"ok": True, "related_subs": related, "key_words": generators.get("key_words"),
                      "generators": generators.get("gens")})


@app.route("/generators/start", methods=["POST"])
@login_required
def sub_gens_start():
    data = json.loads(request.data)
    sub = data.get("sub")
    if sub:
        posts_generator.states_handler.set_posts_generator_state(sub, S_WORK)
        posts_generator.start_generate_posts(sub)
        return jsonify(**{"ok": True, "state": S_WORK})
    return jsonify(**{"ok": False, "error": "sub is not exists"})


@app.route("/generators/pause", methods=["POST"])
@login_required
def sub_gens_pause():
    data = json.loads(request.data)
    sub = data.get("sub")
    if sub:
        posts_generator.states_handler.set_posts_generator_state(sub, S_SUSPEND, ex=3600 * 24 * 7)
        return jsonify(**{"ok": True, "state": S_SUSPEND})
    return jsonify(**{"ok": False, "error": "sub is not exists"})


@app.route("/generators/del_post", methods=["POST"])
@login_required
def del_post():
    data = json.loads(request.data)
    p_hash = data.get("url_hash")
    if p_hash:
        posts_storage.set_post_state(p_hash, PS_BAD)
        return jsonify(**{"ok": True})
    return jsonify(**{"ok": False, "error": "post url hash is not exists"})


@app.route("/generators/del_sub", methods=["POST"])
@login_required
def del_sub():
    data = json.loads(request.data)
    sub_name = data.get("sub_name")
    if sub_name:
        posts_generator.terminate_generate_posts(sub_name)
        db.remove_sub_for_humans(sub_name)
        posts_storage.remove_posts_of_sub(sub_name)
        posts_generator.states_handler.remove_post_generator(sub_name)
        return jsonify(**{"ok": True})

    return jsonify(**{"ok": False, "error": "sub is not exists"})


@app.route("/generators/prepare_for_posting", methods=["POST"])
@login_required
def prepare_for_posting():
    data = json.loads(request.data)
    sub = data.get("sub")
    if sub:
        posts_storage.move_posts_to_ready_state(sub)
        return jsonify(**{"ok": True})

    return jsonify(**{"ok": False, "error": "sub is not exists"})


@app.route("/generators/start_all", methods=["POSt"])
@login_required
def start_all():
    def f():
        subs = db.get_subs_of_all_humans()
        pg = PostsGenerator()
        for sub in subs:
            pg.states_handler.set_posts_generator_state(sub, S_WORK)
            pg.start_generate_posts(sub)
            while 1:
                if posts_generator.states_handler.get_posts_generator_state(sub) == S_END:
                    break
                else:
                    time.sleep(1)

    p = Process(target=f)
    p.start()

    return jsonify(**{"ok": True})


@app.route("/queue/posts/<name>", methods=["GET"])
@login_required
def queue_of_posts(name):
    queue = posts_storage.get_ready_posts(name=name)
    return render_template("posts_queue.html", **{"human_name": name, "queue": queue})


pd = ProcessDirector("server")
im_po_su = ImportantYoutubePostSupplier()
if not pd.is_aspect_work(IMPORTANT_POSTS_SUPPLIER_PROCESS_ASPECT, timing_check=False):
    im_po_su.start()


@app.route("/load_important", methods=["POST"])
def load_important():
    data = json.loads(request.data)
    if "key" in data:
        count_loaded, e = im_po_su.load_new_posts_for_human(data.get("name"), data.get("channel_id"))
        if e:
            return jsonify(**{"ok": False, "error": e})
        return jsonify(**{"ok": True, "key": data.get("key"), "loaded": count_loaded})
    return jsonify(**{"ok": False, "fuck off": "вы кто такие я вас не звал идите нахуй"})


@app.route("/youtube", methods=["POST", "GET"])
@login_required
def youtube_manage():
    return render_template("youtube_manage.html")


if __name__ == '__main__':

    port = 65010
    while 1:
        print port
        try:
            app.run(port=port)
        except:
            port += 1
