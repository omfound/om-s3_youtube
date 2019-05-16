#!/usr/bin/env python3

from s3_api_utils import S3Utils
from granicus_api import GranicusUtils
from db import YTMigration
from pprint import pprint

def debug_agendas():
    client = YTMigration().clientGet("takomapark@openmediafoundation.org")
    s3 = S3Utils(
        client["s3_access_key"],
        client["s3_secret_key"],
        client["s3_bucket"])
    granicus = GranicusUtils(client, s3)
    sessions = YTMigration().sessionsGet(client["id"])

    for index,session in enumerate(sessions):
        folder = granicus.keyFolderGet(session["s3_key"])
        pprint(folder)
        agenda_items = granicus.metadataGet(folder, "agenda") 
        if agenda_items:
            pprint(session)
            pprint(agenda_items)
            break

debug_agendas()
