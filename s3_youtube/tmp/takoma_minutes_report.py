#!/usr/bin/env python3

from db import YTMigration
from s3_api_utils import S3Utils

from pprint import pprint

def takoma_minutes_report():
    client = YTMigration().clientGet("takomapark@openmediafoundation.org")
    s3 = S3Utils(
        client["s3_access_key"],
        client["s3_secret_key"],
        client["s3_bucket"])
    
    sessions = YTMigration().sessionsGet(client["id"])

    for session in sessions:
        if session["status"] == "uploaded":
            has_minutes = False
            if session["documents"]:
                for index, doc in enumerate(session["documents"]):
                    if doc["type"] == "minutes_html" or doc["type"] == "minutes":
                        has_minutes = True

            if not has_minutes:
                if session["session_date"] and session["title"]:
                    print(session["session_date"]+": "+session["title"])
                else:
                    print("NO DATE: "+session["title"])

takoma_minutes_report()
