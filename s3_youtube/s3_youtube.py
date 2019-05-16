#!/usr/bin/env python3

import os
import csv
from urllib import parse
import datetime
from pytz import timezone
from dateutil import parser
from pprint import pprint

#open.media modules
from db import YTMigration
from s3_api_utils import S3Utils
from settings import get_settings
import youtube_api as yt_api_utils


def parse_csv(path):
    with open(path, "r") as f:
        reader = csv.DictReader(f)
        data = list(reader)
    
    if data:
        return data
    else:
        return False


def granicus_parse_url(url):
    parsed = parse.urlparse(url)
    clip_id = parse.parse_qs(parsed.query)['clip_id']

    if clip_id:
        return int(clip_id[0])
    else:
        return False 


def granicus_manual_csv(migration_client, mode):
    granicus_ids = []

    if mode == "include":
        filename = "include.csv"
    elif mode == "exclude":
        filename = "exclude.csv"
    else:
        return None

    path = "/home/ubuntu/s3_youtube/client_data/"+migration_client["email"]+"/"+filename

    if os.path.isfile(path):
        data = parse_csv(path)
        if data:
            for row in data:
                if "url" in row:
                    granicus_id = granicus_parse_url(row["url"])
                    if granicus_id:
                        granicus_ids.append(granicus_id)
                elif "id" in row:
                    granicus_ids.append(int(row["id"]))

    if granicus_ids:
        return granicus_ids

    return None


def granicus_clean_filename(s3_filename):
    filename = s3_filename.replace('"', '')
    filename = filename.replace("'", '')
    filename = filename.replace("/", '_')
    return filename


def s3_download_file(client_data, s3, session):
    download_path = "/nvme/"+granicus_clean_filename(session['s3_filename'])
    download_path = download_path.replace(" ", "")

    s3.fileDownload(session['s3_key'], download_path)
    
    session["local_path"] = download_path
    message = "Downloaded successfully to: "+session["local_path"]
    return session


def process_sessions():
    settings = get_settings("Copy videos from a Granicus S3 export to YouTube")
    migration = YTMigration()
    client_data = migration.clientGet(settings["email"])

    s3 = S3Utils(
        client_data["s3_access_key"],
        client_data["s3_secret_key"],
        client_data["s3_bucket"])

    migration_info = {
        'summary': {
            'attempted': 0,
            'uploaded': 0,
            'skipped': 0,
            'failed': 0
        },
        'sessions': {
            'attempted': [],
            'uploaded': [],
            'skipped': [],
            'failed': []
        }
    }

    if not client_data:
        pprint("There are no client credentials in the system for the email address provided")
        return

    if 'token' not in client_data or not client_data["token"]:
        pprint("Please authenticate this client first at https://s3-youtube.open.media")
        return

    youtube_client = yt_api_utils.youtube_client_get(client_data) 

    sessions = migration.sessionsGet(client_data["id"], 'harvested')
    inclusions = granicus_manual_csv(client_data, "include")
    print("Using manual inclusions file, "+str(len(inclusions))+" sessions")
    exclusions = granicus_manual_csv(client_data, "exclude")

    if sessions:
        counter = 0
        limit = settings["limit"]
        for session in sessions:
            if counter >= limit:
                break;

            manual_inclusion = False
            
            #if there are manual inclusions we only do those
            if inclusions and session["session_id"] and int(session["session_id"]) in inclusions:
                manual_inclusion = True
            elif inclusions:
                migration_info["sessions"]["skipped"].append(sessions)
                continue

            if exclusions and session["session_id"] in exclusions:
                migration_info["sessions"]["skipped"].append(sessions)
                continue

            if not manual_inclusion and settings["start_timestamp"]:
                if not session["session_timestamp"] or session["session_timestamp"] < settings["start_timestamp"]:
                    migration_info["sessions"]["skipped"].append(session)
                    continue

            if not manual_inclusion and settings["end_timestamp"]:
                if not session["session_timestamp"] or session["session_timestamp"] > settings["end_timestamp"]:
                    migration_info["sessions"]["skipped"].append(session)
                    continue

            if not manual_inclusion and settings["folder_ids"]:
                if not session["session_folder"] or session["session_folder"] not in settings["folder_ids"]:
                    migration_info["sessions"]["skipped"].append(session)
                    continue

            if settings["commit"]:
                print("Downloading "+session["title"])
                session = s3_download_file(client_data, s3, session)
                response = yt_api_utils.youtube_upload_session(youtube_client, session)

                if response["id"]:
                    session["youtube_id"] = response["id"]
                    session["status"] = 'uploaded'
                    os.remove(session["local_path"])
                    migration.sessionUpdate(session)
                    migration_info["sessions"]["uploaded"].append(session)
                    pprint("Session uploaded: "+session["title"])
                    if "youtube_playlist" in session:
                      yt_api_utils.youtube_add_session_to_playlist(youtube_client, session)
                else:
                    session["status"] = 'upload failed'
                    migration.sessionUpdate(session)
                    pprint("Session upload failed: "+session["title"])
                    migration_info["sessions"]["failed"].append(session)
            else:
                migration_info["sessions"]["attempted"].append(session)

            counter += 1

    migration_info["summary"]["attempted"] = len(migration_info["sessions"]["attempted"])
    migration_info["summary"]["skipped"] = len(migration_info["sessions"]["skipped"])
    migration_info["summary"]["uploaded"] = len(migration_info["sessions"]["uploaded"])
    migration_info["summary"]["failed"] = len(migration_info["sessions"]["failed"])

    if settings["verbose"]:
        print ("Sessions:")
        for session in migration_info["sessions"]["attempted"]:
            if session["session_date"]:
                datetime = parser.parse(session["session_date"])
                utcDate = datetime.astimezone(timezone('UTC'))
                title_date = utcDate.strftime("%m/%d/%Y")
            else:
                title_date = "NO DATE"
            print(str(session["session_folder"])+": "+session["title"]+" - "+title_date)

    print("Summary:")
    pprint(migration_info["summary"])


process_sessions()
