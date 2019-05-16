#!/usr/bin/env python3

import os
import getopt, sys
from validate_email import validate_email
from pprint import pprint

from db import YTMigration
from s3_api_utils import S3Utils
from granicus_api import GranicusUtils
from settings import get_settings


def metadata_minutes_docs(metadata):
    minutes_docs = []
    if metadata:
        for doc in metadata:
            if doc['status'] == "generated" or doc['filename'] == "generated":
                if doc['published'] == "1":
                    #if "minutes" in doc["label"].lower():
                    minutes_docs.append(doc)
    return minutes_docs


def harvest_html_minutes():
    settings = get_settings("Store and transform Granicus html minutes")

    client = YTMigration().clientGet(settings["email"])
    s3 = S3Utils(
        client["s3_access_key"],
        client["s3_secret_key"],
        client["s3_bucket"])
    granicus = GranicusUtils(client, s3)
    sessions = YTMigration().sessionsGet(client["id"])
    
    counter = 0
    for session in sessions:
        if counter >= settings["limit"]:
            break
        if settings["status"]:
            if session["status"] != settings["status"]:
                continue

        key_parts = session["s3_key"].split("/")
        del key_parts[-1]
        session_folder = '/'.join(key_parts)

        metadata = granicus.metadataGet(session_folder, 'document')
        minutes_docs = metadata_minutes_docs(metadata)

        if minutes_docs:
            if len(minutes_docs) > 1:
                print("Multiple minutes documents found for: "+session["session_id"])
                minutes_doc = None
            else:
                minutes_doc = minutes_docs[0]
        else:
            minutes_doc = None
        
        if minutes_doc:
            minutes_html = granicus.htmlMinutesTransform(session["session_id"], minutes_doc["id"])
            if minutes_html:
                print("Minutes Generated: "+session["session_id"]+": "+session["title"])
                if not settings["commit"]:
                    counter += 1
                    continue

                storage_folder = "/nvme/client_files/"+client["email"]
                minutes_folder = storage_folder+"/documents/minutes_html"
                minutes_filename = session["session_id"]+"_minutes.html"
                minutes_path = minutes_folder+"/"+minutes_filename
                minutes_url = "https://s3-youtube.open.media/client_files/"+client["email"]+"/documents/minutes_html/"+minutes_filename

                os.makedirs(minutes_folder, exist_ok=True)
                html_file = open(minutes_path,"w")
                html_file.write(minutes_html)
                html_file.close()

                print("Stored: "+minutes_path)
                counter += 1


harvest_html_minutes()
