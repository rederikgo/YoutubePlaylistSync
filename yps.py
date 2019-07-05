import datetime
import logging
import logging.handlers
import sqlite3
import sys

import httplib2
from oauth2client.file import Storage
from apiclient.discovery import build
from oauth2client.client import flow_from_clientsecrets
from oauth2client.tools import run_flow
import yaml

class YoutubePlaylists():
    def __init__(self, CLIENT_SECRETS_FILE, CREDENTIALS_FILE):
        logger = logging.getLogger(__name__)
        # Login or create credentials
        YOUTUBE_SCOPE = "https://www.googleapis.com/auth/youtube"
        YOUTUBE_API_SERVICE_NAME = "youtube"
        YOUTUBE_API_VERSION = "v3"

        storage = Storage(CREDENTIALS_FILE)
        credentials = storage.get()
        if credentials is None or credentials.invalid:
            logger.info('Credentials invalid. Initing oAuth flow...')
            flow = flow_from_clientsecrets(CLIENT_SECRETS_FILE, scope=YOUTUBE_SCOPE, message='Nop')
            credentials = run_flow(flow, storage)
            logger.info('Credentials updated')

        self.service = build(YOUTUBE_API_SERVICE_NAME, YOUTUBE_API_VERSION, http=credentials.authorize(httplib2.Http()))
        

    def _request_youtube(self, request):
        try:
            response = request.execute()
            if 'error' in response:
                error_code = response['error']['errors']['code']
                error_message = response['error']['errors']['message']
                raise ValueError('API error {} - {}'.format(error_code, error_message))
            else:
                return response
        except:
            raise ValueError('An error occured during interaction with Youtube: {}'.format(sys.exc_info()))

    def _get_all_pages(self, request_func, kwargs):
        request = request_func(**kwargs)
        response = self._request_youtube(request)
        responses = response['items']

        while 'nextPageToken' in response.keys():
            kwargs['pageToken'] = response['nextPageToken']
            request = request_func(**kwargs)
            response = self._request_youtube(request)
            responses += response['items']

        return responses

    def get_playlists_list(self):
        playlists = [['liked', 'liked']]

        request_playlists = self.service.playlists().list
        kwargs = {'part': 'snippet', 'mine': True}
        playlists_raw = self._get_all_pages(request_playlists, kwargs)

        for playlist in playlists_raw:
            playlist_id = playlist['id']
            playlist_title = playlist['snippet']['title']
            playlists.append([playlist_id, playlist_title])

        return playlists

    def get_videos_list(self, playlist_id):
        videos = []

        if playlist_id == 'liked':
            request_liked = self.service.videos().list
            kwargs = {'part': 'snippet', 'myRating': 'like'}
            videos_raw = self._get_all_pages(request_liked, kwargs)
        else:
            request_videos = self.service.playlistItems().list
            kwargs = {'part': 'snippet', 'playlistId': playlist_id}
            videos_raw = self._get_all_pages(request_videos, kwargs)

        for video in videos_raw:
            video_id = video['id']
            video_title = video['snippet']['title']
            video_descr = video['snippet']['description']
            videos.append([video_id, video_title, video_descr])

        return videos

class db():
    def __init__(self, DB_PATH):
        self.con = sqlite3.connect(DB_PATH)
        self.cur = self.con.cursor()

    def close(self):
        self.cur.close()
        self.con.close()

    def get_playlists(self):
        self.cur.execute("""
            SELECT playlists.playlist_id, playlists.playlist_title
            FROM playlists
        """)
        return self.cur.fetchall()

    def get_videos(self, playlist_id):
        self.cur.execute("""
            SELECT videos.video_id, videos.video_title
            FROM videos
            WHERE videos.playlist_id = ?
        """, (playlist_id, ))
        return self.cur.fetchall()

    def get_video_details(self, video_id):
        self.cur.execute("""
            SELECT videos.video_title, playlists.playlist_title
            FROM videos
            JOIN playlists ON playlists.playlist_id = videos.playlist_id
            WHERE videos.video_id = ?
        """, (video_id, ))
        return self.cur.fetchall()

    def add_playlist(self, playlist_id, playlist_title):
        self.cur.execute("""
            INSERT INTO playlists (playlist_id, playlist_title)
            VALUES (?, ?)
        """, (playlist_id, playlist_title))


    def add_video(self, video_id, video_title, video_descr, playlist_id):
        self.cur.execute("""
            INSERT INTO videos (video_id, video_title, video_descr, playlist_id) VALUES (?, ?, ?, ?)
        """, (video_id, video_title, video_descr, playlist_id))

    def remove_playlist(self, playlist_id):
        self.cur.execute("""
            DELETE FROM videos
            WHERE videos.playlist_id = ?
        """, (playlist_id, ))
        self.cur.execute("""
            DELETE FROM playlists
            WHERE playlists.playlist_id = ?
        """, (playlist_id, ))

    def remove_video(self, video_id):
        self.cur.execute("""
            DELETE FROM videos
            WHERE videos.video_id = ?
        """, (video_id, ))
    
    def get_deleted(self, playlist_id):
        self.cur.execute("""
            SELECT videos.video_id 
            FROM videos 
            WHERE videos.playlist_id = ? AND videos.is_deleted = 'true'
        """, (playlist_id, ))
        return self.cur.fetchall()        
        
    def mark_as_deleted(self, video_id):
        self.cur.execute("""
            UPDATE videos 
            SET is_deleted = 'true' 
            WHERE video_id = ?
        """, (video_id, ))        

    def commit(self):
        self.con.commit()

def main():
    CLIENT_SECRETS_FILE = 'client_id.json'
    CREDENTIALS_FILE = 'creds.json'
    DB_PATH = 'playlists.db'
    REPORT_PATH = 'deleted_videos.txt'
    DEBUG_LEVEL = logging.DEBUG
    
    logger = setup_logger(DEBUG_LEVEL)
    logger.info('SESSION STARTED')
    
    playlists_added = 0
    playlists_removed = 0
    videos_added = 0
    videos_removed = 0
    videos_marked_as_deleted = 0
    
    youtube = YoutubePlaylists(CLIENT_SECRETS_FILE, CREDENTIALS_FILE)
    database = db(DB_PATH)

    # Get youtube playlists list
    youtube_playlists = youtube.get_playlists_list()
    youtube_playlists_ids = [i[0] for i in youtube_playlists]
    db_playlists = database.get_playlists()
    db_playlists_ids = [i[0] for i in db_playlists]

    # Iterate thru playlists
    for playlist in youtube_playlists:
        playlist_id = playlist[0]
        playlist_title = playlist[1]

        # Detect and add new playlist to db
        if playlist_id not in db_playlists_ids:
            database.add_playlist(playlist_id, playlist_title)
            playlists_added += 1
            logger.debug('Playlist added: [{}]'.format(playlist_title))

        # Detect and add new videos to the playlist in the db
        youtube_videos = youtube.get_videos_list(playlist_id)
        youtube_videos_ids = [i[0] for i in youtube_videos]
        db_videos = database.get_videos(playlist_id)
        db_videos_ids = [i[0] for i in db_videos]
        db_deleted_videos = database.get_deleted(playlist_id)
        db_deleted_videos_ids = [i[0] for i in db_deleted_videos]
        for video in youtube_videos:
            video_id = video[0]
            video_title = video[1]
            video_descr = video[2]
            if video_id not in db_videos_ids:
                database.add_video(video_id, video_title, video_descr, playlist_id)
                videos_added += 1
                logger.debug('Video added: [{}]'.format(video_title))

            # Detect new 'deleted and private' videos and mark them in db
            if video_title in ['Deleted video', 'Private video']:
                if video_id not in db_deleted_videos_ids:
                    video_details = database.get_video_details(video_id)
                    deleted_video_title = video_details[0][0]
                    deleted_video_playlist = video_details[0][1]
                    database.mark_as_deleted(video_id)
                    report_deleted_to_file(REPORT_PATH, deleted_video_title, deleted_video_playlist)
                    videos_marked_as_deleted += 1
                    logger.debug('Video marked: [{}]'.format(deleted_video_title))

        # # Mark videos if deleted from youtube
        for video in db_videos:
            video_id = video[0]
            video_title = video[1]
            if video_id not in youtube_videos_ids:
                database.mark_as_deleted(video_id)
                videos_marked_as_deleted += 1
                logger.debug('Video marked: [{}]'.format(video_title))

        database.commit()

    # Remove playlists from db if deleted from youtube
    for playlist in db_playlists:
        playlist_id = playlist[0]
        playlist_title = playlist[1]
        if playlist_id not in youtube_playlists_ids:
            # Count and log videos in the playlist to be deleted
            db_videos = database.get_videos(playlist_id)
            for video in db_videos:
                video_title = video[1]
                logger.debug('Video removed: [{}]'.format(video_title))
                videos_removed += 1
            
            database.remove_playlist(playlist_id)
            playlists_removed += 1
            logger.debug('Playlist removed: [{}]'.format(playlist_title))
    database.commit()
    
    logger.info('Playlists added/removed: {}/{}'.format(playlists_added, playlists_removed))
    logger.info('Videos added/removed: {}/{}'.format(videos_added, videos_removed))
    logger.info('Videos marked as deleted: {}'.format(videos_marked_as_deleted))
    logger.info('SESSION FINISHED')

def report_deleted_to_file(REPORT_PATH, deleted_video_title, deleted_video_playlist):
    with open(REPORT_PATH, 'a') as file:
        current_utc = datetime.datetime.utcnow()
        report_time = current_utc.strftime('%Y-%m-%d %H:%M:%S')
        report_line = '{}: [{}] [{}]\n'.format(report_time, deleted_video_playlist, deleted_video_title)
        file.write(report_line)

def setup_logger(DEBUG_LEVEL):
    formatter = logging.Formatter('%(asctime)s: %(message)s')
    
    handler = logging.handlers.RotatingFileHandler('yps.log', mode='a', maxBytes=10485760, backupCount=0, encoding='utf-8')
    handler.setLevel(DEBUG_LEVEL)
    handler.setFormatter(formatter)
    
    logger = logging.getLogger(__name__)
    logger.setLevel(DEBUG_LEVEL)
    logger.addHandler(handler)
    
    return logger
 

main()
quit