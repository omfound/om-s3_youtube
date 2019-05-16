#!/usr/bin/env python3

from db import YTMigration
from s3_api_utils import S3Utils

import os
import csv
from urllib import parse
from pprint import pprint

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

def set_inclusions_folder():
    client = YTMigration().clientGet("takomapark@openmediafoundation.org")
    sessions = YTMigration().sessionsGet(client["id"], 'harvested')
    inclusions = granicus_manual_csv(client, "include")

    counter = 0
    for session in sessions:
        #if there are manual inclusions we only do those
        if inclusions and session["session_id"] and int(session["session_id"]) in inclusions:
            manual_inclusion = True
        elif inclusions:
            continue

        session_folder = {
            's3_key': session["s3_key"],
            'client_id': session["client_id"],
            'session_folder': 999,
            'category': "Special Meetings"}
        counter += 1
        YTMigration().sessionUpdate(session_folder)
    
    print("Updated "+str(counter)+" session folders.")


set_inclusions_folder()
