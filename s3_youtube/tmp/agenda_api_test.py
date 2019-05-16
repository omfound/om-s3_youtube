#!/usr/bin/env python3

from db import YTMigration
from s3_api_utils import S3Utils
from granicus_api import GranicusUtils
from pprint import pprint

def get_meeting():
    client = YTMigration().clientGet("takomapark@openmediafoundation.org")

    s3 = S3Utils(
        client["s3_access_key"],
        client["s3_secret_key"],
        client["s3_bucket"])

    granicus = GranicusUtils(client, s3)

    meeting = granicus.apiFetchObject("1933")
    pprint(meeting)

get_meeting()
