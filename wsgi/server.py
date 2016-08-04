# coding=utf-8
import json
import os
import re
from datetime import datetime
from uuid import uuid4

from flask import Flask, logging, request, render_template, session, url_for, g, flash
from flask.json import jsonify
from flask_debugtoolbar import DebugToolbarExtension
from flask_login import LoginManager, login_user, login_required, logout_user
from werkzeug.utils import redirect
from wsgi.db import HumanStorage
from wsgi.rr_people import S_WORK, S_SUSPEND, S_STOP
from wsgi.rr_people.posting import POST_GENERATOR_OBJECTS
from wsgi.rr_people.posting.copy_gen import SubredditsRelationsStore
from wsgi.rr_people.posting.posts import PS_BAD, PS_READY, PostsStorage, PS_PREPARED
from wsgi.rr_people.posting.posts_generator import PostsGenerator
from wsgi.rr_people.posting.posts_managing import ImportantYoutubePostSupplier, NoisePostsAutoAdder
from wsgi.rr_people.states.processes import ProcessDirector
from wsgi.wake_up import WakeUp

__author__ = '4ikist'

import sys

reload(sys)
sys.setdefaultencoding('utf-8')

log = logging.getLogger("web")
cur_dir = os.path.dirname(__file__)
app = Flask("Humans", template_folder=cur_dir + "/templates", static_folder=cur_dir + "/static")

app.secret_key = 'foo bar baz'
app.config['SESSION_TYPE'] = 'filesystem'


def tst_to_dt(value):
    return datetime.fromtimestamp(value).strftime("%H:%M %d.%m.%Y")


def array_to_string(array):
    return " ".join([str(el) for el in array])


app.jinja_env.filters["tst_to_dt"] = tst_to_dt
app.jinja_env.globals.update(array_to_string=array_to_string)

if os.environ.get("test", False):
    log.info("will run at test mode")
    app.config["SECRET_KEY"] = "foo bar baz"
    app.debug = True
    app.config['DEBUG_TB_INTERCEPT_REDIRECTS'] = False
    toolbar = DebugToolbarExtension(app)

url = "http://rr-alexeyp.rhcloud.com"
wu = WakeUp()
wu.store.add_url(url)
wu.daemon = True
wu.start()


@app.route("/wake_up/<salt>", methods=["POST"])
def wake_up(salt):
    return jsonify(**{"result": salt})


@app.route("/wake_up", methods=["GET", "POST"])
def wake_up_manage():
    if request.method == "POST":
        urls = request.form.get("urls")
        urls = urls.split("\n")
        for i, url in enumerate(urls):
            url = url.strip()
            if url:
                wu.store.add_url(url)
                urls[i] = url

        saved_urls = wu.store.get_urls()
        to_delete = set(saved_urls).difference(urls)
        for url in to_delete:
            wu.store.remove_url(url)

    urls = wu.store.get_urls()
    return render_template("wake_up.html", **{"urls": urls})


login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'login'

db = HumanStorage(name="hs server")


class User(object):
    def __init__(self, name, pwd):
        self.id = str(uuid4().get_hex())
        self.auth = False
        self.active = False
        self.anonymous = False
        self.name = name
        self.pwd = pwd

    def is_authenticated(self):
        return self.auth

    def is_active(self):
        return True

    def is_anonymous(self):
        return False

    def get_id(self):
        return self.id


class UsersHandler(object):
    def __init__(self):
        self.users = {}
        self.auth_users = {}

    def get_guest(self):
        user = User("Guest", "")
        user.anonymous = True
        self.users[user.id] = user
        return user

    def get_by_id(self, id):
        found = self.users.get(id)
        if not found:
            found = db.users.find_one({"user_id": id})
            if found:
                user = User(found.get('name'), found.get("pwd"))
                user.id = found.get("user_id")
                self.users[user.id] = user
                found = user
        return found

    def auth_user(self, name, pwd):
        authed = db.check_user(name, pwd)
        if authed:
            user = self.get_by_id(authed)
            if not user:
                user = User(name, pwd)
                user.id = authed
            user.auth = True
            user.active = True
            self.users[user.id] = user
            return user

    def logout(self, user):
        user.auth = False
        user.active = False
        self.users[user.id] = user

    def add_user(self, user):
        self.users[user.id] = user
        db.add_user(user.name, user.pwd, user.id)


usersHandler = UsersHandler()
log.info("users handler was initted")
usersHandler.add_user(User("3030", "89231950908zozo"))


@app.before_request
def load_user():
    if session.get("user_id"):
        user = usersHandler.get_by_id(session.get("user_id"))
    else:
        # user = None
        user = usersHandler.get_guest()
    g.user = user


@login_manager.user_loader
def load_user(userid):
    return usersHandler.get_by_id(userid)


@login_manager.unauthorized_handler
def unauthorized_callback():
    return redirect(url_for('login'))


@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        login = request.form.get("name")
        password = request.form.get("password")
        remember_me = request.form.get("remember") == u"on"
        user = usersHandler.auth_user(login, password)
        if user:
            try:
                login_user(user, remember=remember_me)
                return redirect(url_for("main"))
            except Exception as e:
                log.exception(e)

    return render_template("login.html")


@app.route('/logout')
@login_required
def logout():
    logout_user()
    return redirect(url_for('login'))


@app.route("/")
@login_required
def main():
    if request.method == "POST":
        _url = request.form.get("url")
        wu.what = _url

    user = g.user
    return render_template("main.html", **{"username": user.name})


REDIRECT_URI = "http://rr-alexeyp.rhcloud.com/authorize_callback"
C_ID = None
C_SECRET = None


@app.route("/global_configuration/<name>", methods=["GET", "POST"])
@login_required
def global_configuration(name):
    if request.method == "GET":
        result = db.get_global_config(name)
        if result:
            return jsonify(**{"ok": True, "result": result})
        return jsonify(**{"ok": False, "error": "no config with name %s" % name})
    elif request.method == "POST":
        try:
            data = json.loads(request.data)
            result = db.set_global_config(name, data)
            return jsonify(**{"ok": True, "result": result})
        except Exception as e:
            log.warning(e.message)
            return jsonify(**{"ok": False, "error": e})


@app.route("/noise_auto_adder", methods=["POST"])
@login_required
def noise_auto_add():
    data = json.loads(request.data)
    db.set_global_config(NoisePostsAutoAdder.name, data=data)

    if data.get("on"):
        npa = NoisePostsAutoAdder()
        npa.start()
        return jsonify(**{"ok": True, "started": True, "pid": npa.pid})

    return jsonify(**{"ok": True, "started": False})


# generators
splitter = re.compile('[^\w\d_-]*')

srs = SubredditsRelationsStore("server")
posts_generator = PostsGenerator()
process_director = ProcessDirector("server")
posts_storage = PostsStorage("server")
imposu = ImportantYoutubePostSupplier(ms=db, ps=posts_storage)
imposu.start()


@app.route("/posts")
@login_required
def posts():
    subs = db.get_all_humans_subs()
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
    subs = db.get_all_humans_subs()
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


@app.route("/queue/posts/<name>", methods=["GET"])
@login_required
def queue_of_posts(name):
    queue = posts_storage.get_ready_posts(name=name)
    return render_template("posts_queue.html", **{"human_name": name, "queue": queue})


if __name__ == '__main__':
    print os.path.dirname(__file__)
    app.run(port=65010)
