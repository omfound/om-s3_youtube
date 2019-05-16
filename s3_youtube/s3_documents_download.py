#!/usr/bin/env python3

import os
import getopt, sys
from validate_email import validate_email

from db import YTMigration
from s3_api_utils import S3Utils

from pprint import pprint

# GLOBAL DEFAULTS
HARVEST_LIMIT=1

def get_settings():
    settings = {'limit': HARVEST_LIMIT}

    # read commandline arguments, first
    fullCmdArguments = os.sys.argv

    # - further arguments
    argumentList = fullCmdArguments[1:]
    unixOptions = "hel"
    gnuOptions = ["help", "email=", "limit="]

    try:  
        arguments, values = getopt.getopt(argumentList, unixOptions, gnuOptions)
    except getopt.error as err:  
        # output error, and return with an error code
        print (str(err))
        sys.exit(2)

    # evaluate given options
    argdict = dict(arguments)
    if "--email" not in argdict and "-e" not in argdict:
        print ("Email address for the YouTube account must be provided")
        exit()
    for currentArgument, currentValue in arguments:  
        if currentArgument in ("-h", "--help"):
            print ("Usage: add_playlists [CLIENT_EMAIL] [OPTION]...")
            print ("Download documents from S3")
            print ("\nOptions:")
            print ("e, --email=EMAIL           Email address that identifies the YouTube account")
            print ("l, --limit=LIMIT           Number of harvested videos to process")
        elif currentArgument in ("-e", "--email"):
            if validate_email(currentValue):
                settings["email"] = currentValue
            else:
                print ("The email address provided does not appear to be valid")
                exit()
        elif currentArgument in ("-l", "--limit"):
            if currentValue.strip().isdigit():
                settings["limit"] = int(currentValue.strip())
            else:
                print ("Limit must be an integer")
                exit()

    return settings


def download_documents():
    settings = get_settings()
    migration_client = YTMigration().clientGet(settings["email"])
    sessions = YTMigration().sessionsGet(migration_client["id"], 'uploaded')

    s3 = S3Utils(
        migration_client["s3_access_key"],
        migration_client["s3_secret_key"],
        migration_client["s3_bucket"])

    storage_path = "/nvme/client_files/"+migration_client["email"]
    doc_path = storage_path+"/documents"
    doc_url = "https://s3-youtube.open.media/client_files/"+migration_client["email"]+"/documents"

    counter = 0
    for session in sessions:
        if counter < settings["limit"]:
            if session["documents"]:
                doc_counter = 0
                for index, document in enumerate(session["documents"]):
                    if "path" not in document or not document["path"]:
                        if document["s3_key"]:
                            destination_path = doc_path+"/"+document["type"]+"/"+document["filename"]
                            destination_url = doc_url+"/"+document["type"]+"/"+document["filename"]
                            s3.fileDownload(document["s3_key"], destination_path)
                            session["documents"][index]["path"] = destination_path
                            session["documents"][index]["url"] = destination_url
                            doc_counter += 1

                YTMigration().sessionUpdate(session)
                print ("Downloaded "+str(doc_counter)+" documents for "+session["title"])
                counter += 1

download_documents()
