#!/usr/bin/env python3

import boto3
import botocore
from pprint import pprint
from db import YTMigration
import json


def granicus_clean_filename(s3_filename):
    filename = s3_filename.replace('"', '')
    filename = filename.replace("'", '')
    filename = filename.replace("/", '_')
    return filename


def s3_get_file(client_data, s3_resource, session):
    download_path = "/home/ubuntu/s3_youtube/client_data/"+client_data["email"]+"/files/"+granicus_clean_filename(session['s3_filename'])

    try:
        s3_resource.Bucket(client_data['s3_bucket']).download_file(session['s3_key'], download_path)
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            print("The object does not exist.")
        else:
            raise
    return


def download():
    migration = YTMigration()
    client_data = migration.clientGet("brian@openmediafoundation.org")

    bucket = client_data["s3_bucket"]
    prefix = client_data["s3_prefix"]
    s3_resource = boto3.resource('s3')

    sessions = migration.sessionsGet(client_data["id"], 'harvested', 1)
    if sessions:
        s3_get_file(client_data, s3_resource, sessions[0])

download()
