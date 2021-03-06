import logging
import re

from apiclient.discovery import build
from apiclient.errors import HttpError

from rr_lib.cm import ConfigManager
from wsgi.properties import YOUTUBE_API_VERSION, YOUTUBE_TAG_SUB, YOUTUBE_TAG_TITLE, YOUTUBE_TAG_URL_TIME
from wsgi.rr_people.posting.posts import PostsStorage, PostSource, PS_BAD

log = logging.getLogger("youtube")

YOUTUBE_URL = lambda x: "https://www.youtube.com/watch?v=%s" % x

y_url_re = re.compile(
    "(?:youtube(?:-nocookie)?\.com\/(?:[^\/\n\s]+\/\S+\/|(?:v|e(?:mbed)?)\/|\S*?[?&]v=)|youtu\.be\/)([a-zA-Z0-9_-]{11})")


class YoutubeChannelsHandler(object):
    def __init__(self, ps=None):
        cm = ConfigManager()
        self.youtube = build(cm.get('YOUTUBE_API_SERVICE_NAME'),
                             YOUTUBE_API_VERSION,
                             developerKey=cm.get('YOUTUBE_DEVELOPER_KEY'))
        self.posts_storage = ps or PostsStorage(name="youtube posts supplier")

    def _get_tag_value(self, tags, tag_key):
        for tag in tags:
            if tag_key in tag:
                return tag.replace(tag_key, "").strip()

    def _form_posts_on_videos_info(self, items):
        result = []
        for video_info in items:
            id = video_info.get("id")
            if id:
                tags = video_info.get("snippet", {}).get("tags", [])
                title = self._get_tag_value(tags, YOUTUBE_TAG_TITLE)
                if not title:
                    log.warn("Video [%s] have not pt: tag and title will be real title"%id)
                    title = video_info.get("snippet", {}).get("title")
                sub = self._get_tag_value(tags, YOUTUBE_TAG_SUB)
                if not sub:
                    log.warn("Video [%s] (%s) without sub; Skip this video :(" % (id, title))
                    continue
                url = YOUTUBE_URL(id)
                url_time =  self._get_tag_value(tags, YOUTUBE_TAG_URL_TIME)
                if url_time:
                    url = "%s#t=%s"%(url, url_time)

                ps = PostSource(url=url, title=title, for_sub=sub, video_id=id)
                result.append(ps)
                log.info("Found important post: %s", ps)
            else:
                log.warn("video: \n%s\nis have not id :( " % video_info)
        return result

    def _get_not_loaded_ids(self, video_ids):
        result = []
        for v_id in video_ids:
            data = self.posts_storage.is_video_id_present(v_id)
            if data is not None and data.get("state") == PS_BAD:
                self.posts_storage.delete_post(data.get("_id"))
            elif data is not None:
                continue
            result.append(v_id)
        return result

    def get_new_channel_videos(self, channel_id):
        items = []
        q = {"channelId": channel_id,
             "part": "snippet",
             "maxResults": 50,
             "order": "date"}
        while 1:
            search_result = self.youtube.search().list(**q).execute()
            video_ids = filter(lambda x: x,
                               map(lambda x: x.get("id", {}).get("videoId"),
                                   search_result.get('items', [])))
            new_videos_ids = self._get_not_loaded_ids(video_ids)
            log.info("found %s posts in channel: %s; not saved: %s" % (len(video_ids), channel_id, len(new_videos_ids)))

            if new_videos_ids:
                videos_data = self.youtube.videos().list(
                    **{"id": ",".join(new_videos_ids), "part": "snippet"}).execute()
                prep_videos = self._form_posts_on_videos_info(videos_data.get("items", []))
                items.extend(prep_videos)
            else:
                break

            if not search_result.get("nextPageToken"):
                break
            else:
                q['pageToken'] = search_result.get("nextPageToken")

        return items

    def get_tags_of_video_id(self, video_id):
        videos_data = self.youtube.videos().list(
            **{"id": ",".join([video_id]), "part": "snippet"}).execute()
        prep_videos = self._form_posts_on_videos_info(videos_data.get("items", []))
        return prep_videos

    def get_video_id(self, post_url):
        found = y_url_re.findall(post_url)
        if found:
            found = found[0]
            return found[-1]

    def get_channel_id(self, post_url):
        video_id = self.get_video_id(post_url)
        if not video_id: return
        video_response = self.youtube.videos().list(
            id=video_id,
            part='snippet'
        ).execute()
        for item in video_response.get('items'):
            snippet = item.get("snippet")
            return snippet.get("channelId")
