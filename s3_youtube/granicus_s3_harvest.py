#!/usr/bin/env python3

from pprint import pprint
import json
import os.path

from settings import get_settings
from db import YTMigration
from s3_api_utils import S3Utils
from granicus_api import GranicusUtils


# get the s3 key for the video file defined in the archive.csv metadata
# defaults to the first video in folder if none is defined in archive.csv
def video_get(s3, folder, metadata):
    video = None
    videos = s3.filesGet(folder, "mp4")

    if videos and len(videos) > 0:
        if metadata and metadata[0]["video_file"]:
            video_file = metadata[0]["video_file"]
            video_file_mp4 = video_file.replace(".wmv", ".mp4")

            for key in videos:
                if video_file in key or video_file_mp4 in key:
                    video = key
            if not video:
                print("No video file found in folder matching metadata: "+folder)
                print("Metadata video: " + metadata[0]["video_file"])
                pprint(videos)
        else:
            print("No metadata found, defaulting to first video in folder: "+folder)
            video = videos[0]
    else:
        print("No videos found in folder: "+folder)

    return video


def prepare_session(client, folder, video, metadata, api_metadata):
    folder_id = None
    folder_label = None
    folder_playlist = None
    granicus_date = None
    description = None

    if not metadata:
        metadata = []
        metadata.append({
            'id': None,
            'date': None
        })

    title = folder.replace(client["s3_prefix"], '')
    title = title[:-1]

    if api_metadata:
        if 'folder_id' in api_metadata and api_metadata['folder_id']:
            folder_id = api_metadata['folder_id']
            # unfortunately there is no way to get any details about folders
            # from the granicus api so instead we reference a manual mapping
            # file in the client file folder
            folder_data = granicus_folder_data(client, folder_id)
            if folder_data:
                if "label" in folder_data:
                    folder_label = folder_data["label"]

                if "youtube_playlist" in folder_data:
                    folder_playlist = folder_data["youtube_playlist"]

        title = api_metadata["name"]
        if "datetime" in api_metadata:
            granicus_date = api_metadata["datetime"]
        if "description" in api_metadata:
            description = api_metadata["description"]

    # get the raw filename by stripping out the prefix from the s3 key
    filename = video.replace(client["s3_prefix"], '')
    session = {
        'title': title,
        's3_key': video,
        's3_filename': filename,
        'session_id': metadata[0]['id'],
        'session_date': granicus_date,
        'session_folder': folder_id,
        'session_description': description,
        'category': folder_label,
        'youtube_playlist': folder_playlist}

    return session


def sessions_get(s3, client, granicus, folders, limit):
    sessions = []
    counter = 0

    # each folder holds all of the files and metadata for a session
    for folder in folders:
        if counter >= limit:
            break

        # metadata is stored in archive.csv inside the folder
        metadata = granicus.metadataGet(folder)

        # exported metadata is incomplete, so we fetch additional information
        # from the granicus public api
        if metadata and metadata[0]['id']:
            api_metadata = granicus.apiFetchObject(metadata[0]['id'])
        else:
            api_metadata = None 

        video = video_get(s3, folder, metadata)

        if video:
            session = prepare_session(client, folder, video, metadata, api_metadata)
            sessions.append(session)
            if session['session_id'] is None:
                id_label = 'None'
            else:
                id_label = session['session_id']
            print("Harvested: "+id_label+" - "+session["title"])

        counter += 1
    return sessions


def granicus_folder_data(client, folder_id):
    label_path = "/home/ubuntu/s3_youtube/client_data/"+client["email"]+"/granicus_folders.json"
    folder_id = str(folder_id)

    if os.path.isfile(label_path):
        with open(label_path) as json_file:
            data = json.load(json_file)
            if folder_id in data['folders']:
                return data['folders'][folder_id]

    return None


def sessions_filter_settings(settings, sessions):
    filtered_sessions = []

    for session in sessions:
        if settings["start_timestamp"]:
            if not session["session_timestamp"] or session["session_timestamp"] < settings["start_timestamp"]:
                continue
        if settings["end_timestamp"]:
            if not session["session_timestamp"] or session["session_timestamp"] > settings["end_timestamp"]:
                continue
        if settings["folder_ids"]:
            if not session["session_folder"] or session["session_folder"] not in settings["folder_ids"]:
                continue
        filtered_sessions.append(session)

    return filtered_sessions


def sessions_store(client, sessions):
    for session in sessions:
        existing_session = YTMigration().sessionGet(client["id"], session)
        if not existing_session:
            YTMigration().sessionStore(client["id"], session)
    return


def harvest():
    settings = get_settings("Harvest basic session information from Granicus S3 export")
    client = YTMigration().clientGet(settings["email"])

    s3 = S3Utils(
        client["s3_access_key"],
        client["s3_secret_key"],
        client["s3_bucket"])
    granicus = GranicusUtils(client, s3)

    folders = granicus.foldersGet() 

    sessions = sessions_get(s3, client, granicus, folders, settings["limit"])
    print("Total sessions found: "+str(len(sessions)))

    sessions = sessions_filter_settings(settings, sessions)
    print("Total sessions to be stored after filtering by settings: "+str(len(sessions)))

    if settings["commit"]:
        sessions_store(client, sessions)
        print("Sessions stored: "+str(len(sessions)))
    else:
        print("Add --commit to command line store sessions")


harvest()
