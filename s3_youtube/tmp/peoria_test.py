#!/usr/bin/env python3

from s3_api_utils import S3Utils
from granicus_api import GranicusUtils
from db import YTMigration
from pprint import pprint

def get_metadata():
    client = YTMigration().clientGet("peoria@openmediafoundation.org")

    s3 = S3Utils(
        client["s3_access_key"],
        client["s3_secret_key"],
        client["s3_bucket"])

    granicus = GranicusUtils(client, s3)

    session = granicus.apiFetchObject("2729")
    pprint(session)

get_metadata()
