# coding=utf-8
import logging
import os
import sys

__author__ = 'alesha'

def module_path():
    if hasattr(sys, "frozen"):
        return os.path.dirname(
            sys.executable
        )
    return os.path.dirname(__file__)


cacert_file = os.path.join(module_path(), 'cacert.pem')

logger = logging.getLogger()

logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s[%(levelname)s]%(name)s|%(processName)s(%(process)d): %(message)s')
formatter_process = logging.Formatter('%(asctime)s[%(levelname)s]%(name)s|%(processName)s: %(message)s')
formatter_human = logging.Formatter('%(asctime)s[%(levelname)s]%(name)s|%(processName)s: %(message)s')

sh = logging.StreamHandler()
sh.setFormatter(formatter)
logger.addHandler(sh)

logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("werkzeug").setLevel(logging.WARNING)

redis_max_connections = 2

SEC = 1
MINUTE = 60
HOUR = MINUTE * 60
DAY = HOUR * 24
WEEK = DAY * 7
WEEK_DAYS = {0: "MO", 1: "TU", 2: "WE", 3: "TH", 4: "FR", 5: "SA", 6: "SU"}

DEFAULT_LIMIT = 500
# DEFAULT_LIMIT = 20

# youtube props
YOUTUBE_API_SERVICE_NAME = "youtube"
YOUTUBE_API_VERSION = "v3"
YOUTUBE_TAG_SUB = "sub:"
YOUTUBE_TAG_TITLE = "pt:"

force_post_manager_sleep_iteration_time = 5 * MINUTE  # время через которое он будет сканировать ютуб

test_mode = os.environ.get("RR_TEST", "false").strip().lower() in ("true", "1", "yes")
print "TEST? ", test_mode

logger.info(
    "Reddit People MANAGEMENT SYSTEM STARTED... \nEnv:%s" % "\n".join(
        ["%s:\t%s" % (k, v) for k, v in os.environ.iteritems()]))
