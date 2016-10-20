import re
import time
from collections import Counter, defaultdict
from datetime import datetime

from apiclient.discovery import build
from apiclient.errors import HttpError

from stop_words import get_stop_words
from stemming.porter2 import stem

from rr_lib.cm import ConfigManager
from wsgi.db import DBHandler
from wsgi.properties import YOUTUBE_API_SERVICE_NAME, YOUTUBE_API_VERSION
from wsgi.youtube.store import YMStorage

token_reg = re.compile("[\\W\\d]+")
stop_words_en = get_stop_words("en")

_bad_words = {"http", "https", "com", "info", "play", "game", "www", "follow", "find", }

__doc__ = """
c_filter =
'commentCount_f'
'commentCount_t'

'subscriberCount_f'
'subscriberCount_t'

'videoCount_f'
'videoCount_t'

'viewCount_f'
'viewCount_t'


 v_filter =
'commentCount_f'
'commentCount_t'

'dislikeCount_f'
'dislikeCount_t'

'favoriteCount_f'
'favoriteCount_t'

'likeCount_f'
'likeCount_t'

'viewCount_f'
'viewCount_t'


'time_to_see'


agg_filter=
w_f
w_t

t_f
t_f
"""


def calc_avg_speed(speeds):
    return sum(speeds) / float(len(speeds))


def calc_rate(statistics):
    return float(statistics.get("viewCount", 1))


def calc_speed(video_data):
    return video_data.get("rate") / video_data.get("age")


def normalize(sentence, bad_words=_bad_words):
    res = set()
    if isinstance(sentence, (str, unicode)):
        tokens = token_reg.split(sentence.lower().strip())
        for token in tokens:
            if len(token) > 2 and token not in bad_words:
                stemmed = stem(token)
                if stemmed not in stop_words_en:
                    res.add(token)
    return res


def aggregate_videos_datas(videos_datas):
    tags_speeds = defaultdict(list)
    words_speeds = defaultdict(list)
    for video_data in videos_datas:
        v_speed = calc_speed(video_data)
        for tag in video_data.get("tags"):
            tags_speeds[tag].append(v_speed)
        for word in video_data.get("words"):
            words_speeds[word].append(v_speed)

    tags_result = {}
    words_result = {}

    for tag, speeds in tags_speeds.iteritems():
        tags_result[tag] = calc_avg_speed(speeds)

    for word, speeds in words_speeds.iteritems():
        words_result[word] = calc_avg_speed(speeds)

    return tags_result, words_result


def aggregate_channels_datas(self, channels_datas):
    pass


class YoutubeManager(object):
    def __init__(self):
        cm = ConfigManager()
        self.youtube = build(YOUTUBE_API_SERVICE_NAME, YOUTUBE_API_VERSION,
                             developerKey=cm.get('YOUTUBE_DEVELOPER_KEY'))
        self.store = YMStorage()

    def get_ids_of_channels(self, filter_data):
        q = {
            'q': filter_data.get('keywords'),
            'part': "id,snippet",
            'maxResults': 50,
            'order': 'viewCount',
            'type': 'channel,video'
        }
        channel_id = lambda channel_info: channel_info.get("snippet", {}).get("channelId")
        for channels_bach in self._search_iter(q):
            channels_datas = []
            loaded_channel_ids = map(channel_id, filter(channel_id, channels_bach))
            stored, addled = self.store.get_addled_channels(loaded_channel_ids)
            channels_datas.extend(stored)

            if addled:
                updated_channels_datas = self._load_new_channels_datas(addled, filter_data)
                channels_datas.extend(updated_channels_datas)

            for channels_data in channels_datas:
                yield channels_data.get("channel_id"), channels_data

    def _load_new_channels_datas(self, channel_ids, filter_data):
        response = self.youtube.channels().list(part="statistics,snippet", id=",".join(channel_ids)).execute()
        items = response.get('items', [])
        result = []
        for channel_data in items:
            statistics = channel_data.get("statistics")
            snippet = channel_data.get("snippet")
            if not statistics or not snippet or \
                    not self._check_stat(statistics, filter_data.get('c_filter')) or \
                    not self._check_words(" ".join([snippet.get("title"), snippet.get("description")]),
                                          filter_data.get("m_keywords")):
                continue
            channel_id = channel_data.get("id")
            info = dict(statistics, **snippet)
            self.store.update_channel(channel_id, info, filter_data.get('exp_id'))
            result.append(dict({"channel_id": channel_id}, **info))
        return result

    def get_channel_videos(self, channel_id, v_filter, time_to_see):
        q = {
            "channelId": channel_id,
            "type": "video",
            "part": "snippet",
            "maxResults": 50,
            "order": "viewCount"
        }
        pub_at = lambda video_info: video_info.get("snippet", {}).get("publishedAt")
        video_id = lambda video_info: video_info.get("id", {}).get("videoId")

        for video_bach in self._search_iter(q):
            videos_datas = []
            new_video_ids = map(lambda x: x[0],
                                filter(lambda x: time.time() - x[1] < time_to_see,
                                       map(lambda video_info: (video_id(video_info),
                                                               time.mktime(datetime.strptime(pub_at(video_info),
                                                                                             "%Y-%m-%dT%H:%M:%S.000Z").timetuple())),
                                           filter(lambda x: pub_at(x) and video_id(x), video_bach))))

            stored, addled = self.store.get_addled_videos(new_video_ids)
            videos_datas.extend(stored)
            if addled:
                new_videos_datas = self._load_new_videos_datas(addled, v_filter)
                videos_datas.extend(new_videos_datas)

            for video_data in videos_datas:
                yield video_data

    def _load_new_videos_datas(self, video_ids, v_filter):
        cur_load_videos_data = self.youtube.videos().list(
            **{"id": ",".join(video_ids), "part": "snippet,statistics"}).execute()
        load_videos_data = cur_load_videos_data.get("items", [])
        videos_datas = []
        for video in load_videos_data:
            snippet = video.get("snippet", {})
            statistics = video.get("statistics", {})
            if snippet and statistics and self._check_stat(statistics, v_filter):
                publishedAt = snippet.get("publishedAt")
                video_data = {"tags": snippet.get("tags"),
                              "words": list(normalize(" ".join([snippet.get("title"), snippet.get("description")]))),
                              "age": time.time() - time.mktime(
                                  datetime.strptime(publishedAt, "%Y-%m-%dT%H:%M:%S.000Z").timetuple()),
                              "rate": calc_rate(statistics),
                              "video_id": video.get("id"),
                              "channel_id": snippet.get("channelId"),
                              "thumb": snippet.get("thumbnails", {}).get("default", {}).get("url"),
                              "title": snippet.get("title"),
                              "description": snippet.get("description")
                              }
                self.store.update_video(video.get("id"), video_data)
                videos_datas.append(video_data)
        return videos_datas

    def _check_words(self, sentence, minus_words):
        return len(normalize(sentence).intersection(set(minus_words))) == 0

    def _check_stat(self, stat, filter_data):

        for k, v in stat.iteritems():
            f_k = "%s_f" % k
            t_k = "%s_t" % k
            if f_k in filter_data and t_k in filter_data:
                v = int(v)
                if v < filter_data[f_k] or v > filter_data[t_k]:
                    return False
                else:
                    stat[k] = v
        return True

    def _search_iter(self, q):
        while 1:
            search_result = self.youtube.search().list(**q).execute()
            items = search_result.get("items", [])
            if items:
                yield items
            else:
                break

            if not search_result.get("nextPageToken"):
                break
            else:
                q['pageToken'] = search_result.get("nextPageToken")


if __name__ == '__main__':
    ym = YoutubeManager()
    c_filter = {
        'commentCount_f': 0,
        'commentCount_t': 9999999999999,

        'subscriberCount_f': 0,
        'subscriberCount_t': 9999999999999,

        'videoCount_f': 100,
        'videoCount_t': 9999999999999,

        'viewCount_f': 10000000,
        'viewCount_t': 9223372036854775807,
    }
    v_filter = {
        'commentCount_f': 0,
        'commentCount_t': 9223372036854775807,

        'dislikeCount_f': 0,
        'dislikeCount_t': 9223372036854775807,

        'favoriteCount_f': 0,
        'favoriteCount_t': 9223372036854775807,

        'likeCount_f': 0,
        'likeCount_t': 9223372036854775807,

        'viewCount_f': 100000,
        'viewCount_t': 9223372036854775807,
    }

    agg_filter = {
        'w_f': 0.,
        'w_t': 10000.,

        't_f': 0.,
        't_t': 1000.,
    }


    for channel_id, info in ym.get_ids_of_channels(filter_data):
        videos = ym.get_channel_videos(channel_id, filter_data.get("v_filter"), DEFAULT_TIME_TO_SEE)
        tags, words = aggregate_videos_datas(videos)
        print channel_id
        print info
        print tags
        print words
