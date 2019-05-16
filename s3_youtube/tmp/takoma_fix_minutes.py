#!/usr/bin/env python3

from db import YTMigration
from s3_api_utils import S3Utils

from pprint import pprint

def fix_tacoma_doc():
    client = YTMigration().clientGet("takomapark@openmediafoundation.org")
    s3 = S3Utils(
        client["s3_access_key"],
        client["s3_secret_key"],
        client["s3_bucket"])
    
    sessions = YTMigration().sessionsGet(client["id"])

    for session in sessions:
        update_docs = False
        if session["documents"]:
            for index, doc in enumerate(session["documents"]):
                if doc["type"] == "minutes_html":
                    del(session["documents"][index])
                    update_docs = True

            if update_docs:
                session_docs = {
                    's3_key': session["s3_key"],
                    'client_id': session["client_id"],
                    'documents': session["documents"]}
                pprint(session_docs)
                YTMigration().sessionUpdate(session_docs)

fix_tacoma_doc()
