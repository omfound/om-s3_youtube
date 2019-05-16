#!/usr/bin/env python3

import os

import boto3
import botocore 
from pprint import pprint


class S3Utils():
    def __init__(self, access_key, secret_key, bucket):
        client = boto3.client(
            's3',
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key)

        resource = boto3.resource(
            's3',
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key)

        self.client = client
        self.resource = resource
        self.bucket = bucket


    def filesGet(self, prefix=None, suffix=None):
        files = []
        kwargs = {'Bucket': self.bucket}

        if prefix:
            kwargs["Prefix"] = prefix

        while True:
            results = self.client.list_objects_v2(**kwargs)

            for result in results['Contents']:
                if suffix:
                    if result['Key'].endswith(suffix):
                        #something wrong here returning multiple strings
                        files.append(result['Key'])
                else:
                    files.append(result['Key'])

            try:
                kwargs['ContinuationToken'] = results['NextContinuationToken']
            except KeyError:
                break

        return files


    def foldersGet(self, prefix=None):
        folders = []
        kwargs = {
            'Bucket': self.bucket,
            'Delimiter': '/'}

        if prefix:
            kwargs["Prefix"] = prefix

        while True:
            results = self.client.list_objects_v2(**kwargs)

            for result in results.get('CommonPrefixes'):
                folders.append(result.get('Prefix'))

            try:
                kwargs['ContinuationToken'] = results['NextContinuationToken']
            except KeyError:
                break

        return folders


    def fileDownload(self, key, destination):
        directory_parts = destination.split("/")
        del directory_parts[-1]
        directory = "/".join(directory_parts)
        os.makedirs(directory, exist_ok=True)

        try:
            self.resource.Bucket(self.bucket).download_file(key, destination)
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                print("The object does not exist.")
            else:
                raise
        return

    
    def keyFilenameGet(self, key):
        key_parts = key.split("/")
        filename = key_parts[-1]

        return filename
