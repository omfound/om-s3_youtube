#!/usr/bin/env python3

from db import YTMigration
from s3_api_utils import S3Utils
from granicus_api import GranicusUtils

from pprint import pprint

def granicus_html():
    client = YTMigration().clientGet("takomapark@openmediafoundation.org")
    pprint(client)
    s3 = S3Utils(
        client["s3_access_key"],
        client["s3_secret_key"],
        client["s3_bucket"])
    #clip_id = 1933
    clip_id = 39432423
    granicus = GranicusUtils(client, s3)
    html_agenda = granicus.htmlAgendaTransform(clip_id)
    print(html_agenda)

granicus_html()
