#!/usr/bin/env python3

import boto3
import botocore 
from pprint import pprint
from db import YTMigration
from s3_api_utils import S3Utils
from granicus_api import GranicusUtils
import json
import os
import getopt, sys
from validate_email import validate_email
import time
import datetime
import dateutil.parser

# GLOBAL DEFAULTS
HARVEST_LIMIT=1


def get_settings():
    settings = {'limit': HARVEST_LIMIT, 'uploaded': False, 'minutes': False}

    # read commandline arguments, first
    fullCmdArguments = os.sys.argv

    # - further arguments
    argumentList = fullCmdArguments[1:]
    unixOptions = "hel"
    gnuOptions = ["help", "email=", "limit=", "uploaded", "minutes"]

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
            print ("Usage: s3_granicus_harvest_docs [CLIENT_EMAIL] [OPTION]...")
            print ("Harvest document information from a Granicus S3 export")
            print ("\nOptions:")
            print ("e, --email=EMAIL           Email address that identifies the YouTube account")
            print ("l, --limit=LIMIT           Number of harvested videos to process")
            print ("u, --uploaded              Only process uploaded sessions")
            print ("m, --minutes               Include minutes")
        elif currentArgument in ("-u", "--uploaded"):
            settings["uploaded"] = True
        elif currentArgument in ("-m", "--minutes"):
            settings["minutes"] = True
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


def session_id_from_granicus_id(client_id, granicus_id):
    session_id = None
    session = YTMigration().sessionGetByExtId(client_id, granicus_id)
    if session:
        session_id = session["id"]
    return session_id


def session_agenda_items_prepare(client, agenda_items):
    session_agenda_items = []

    for agenda_item in agenda_items:
        session_id = session_id_from_granicus_id(client["id"], agenda_item["session_id"])

        #Granicus stores -1 for unstamped agenda items
        start = None
        if agenda_item["start_seconds"] != "-1" and agenda_item["start_seconds"].isdigit():
            start = agenda_item["start_seconds"]

        external_parent_id = None
        if int(agenda_item["parent_item_id"]) != int(agenda_item["session_id"]):
            external_parent_id = agenda_item["parent_item_id"]

        session_agenda_items.append({
            "parent_id": None,
            "session_id": int(session_id),
            "external_session_id": agenda_item["session_id"], 
            "external_id": agenda_item["id"],
            "external_parent_id": external_parent_id,
            "label": agenda_item["label"],
            "end": None,
            "start": start,
            "attachment_label": agenda_item["attachment_label"],
            "attachment_filename": agenda_item["attachment_filename"],
            "type": agenda_item["type"]})

    return session_agenda_items


def attach_documents(client, granicus, s3, settings, sessions):
    for index,session in enumerate(sessions):
        documents = []
        folder = granicus.keyFolderGet(session["s3_key"])
        metadata = granicus.metadataGet(folder, "archive")
        dmeta = granicus.metadataGet(folder, "document")
        agenda_items = granicus.metadataGet(folder, "agenda") 
        files = granicus.documentsGet(folder)

        #agenda
        if metadata:
            agenda_file_name = False
            agenda_url = False
            if metadata[0]["agenda_file_name"]:
                agenda_file_name = metadata[0]["agenda_file_name"]
            elif metadata[0]["agenda_url"]:
                agenda_url = metadata[0]["agenda_url"]

            if agenda_file_name:
                if files:
                    for dindex,dfile in enumerate(files):
                        if agenda_file_name in dfile:
                            filename = s3.keyFilenameGet(dfile)
                            documents.append({
                                "type": "agenda",
                                "location": "internal",
                                "s3_key": dfile,
                                "filename": filename})
                            del(files[dindex])
            elif agenda_url:
                documents.append({
                    "type": "agenda",
                    "location": "external",
                    "s3_key": None,
                    "filename": None,
                    "url": agenda_url})

        #minutes
        if settings["minutes"]:
            if files:
                if dmeta:
                    for dindex, dfile in enumerate(files):
                        for mdoc in dmeta:
                            if mdoc["filename"] in dfile:
                                if mdoc["published"] == "1" and mdoc["hidden"] == "0" and "minute" in mdoc["label"].lower():
                                    filename = s3.keyFilenameGet(dfile)
                                    documents.append({
                                        "type": "minutes",
                                        "location": "internal",
                                        "s3_key": dfile,
                                        "filename": filename 
                                    })
                                    del(files[dindex])
                                    break

        if agenda_items:
            #agenda_items
            sessions[index]["agenda_items"] = session_agenda_items_prepare(client, agenda_items)

            #agenda item attachments
            for agenda_item in agenda_items:
                if agenda_item["attachment_filename"] and files and "Note" not in agenda_item["type"] and "PrivateNote" not in agenda_item["type"]:
                    for dindex,dfile in enumerate(files):
                        if agenda_item["attachment_filename"] in dfile:
                            filename = s3.keyFilenameGet(dfile)
                            if agenda_item["attachment_label"] and agenda_item["attachment_label"].lower() == "meeting agenda":
                                documents.append({
                                    "type": "agenda",
                                    "location": "internal",
                                    "label": agenda_item["attachment_label"],
                                    "s3_key": dfile,
                                    "filename": filename,
                                    "external_agenda_item_id": agenda_item["id"]})
                            else:
                                documents.append({
                                    "type": "agenda_item_attachment",
                                    "location": "internal",
                                    "label": agenda_item["attachment_label"],
                                    "s3_key": dfile,
                                    "filename": filename,
                                    "external_agenda_item_id": agenda_item["id"]})

                            del(files[dindex])

        #report on skipped files
        if files:
            for dfile in files:
                meta_found = False
                if dmeta:
                    for fmeta in dmeta:
                        if fmeta["filename"] in dfile:
                            print ("EXCLUDED: "+dfile)
                            pprint(fmeta)
                            meta_found = True

                if not meta_found:
                    print("EXCLUDED: "+dfile)
                    print("NO META FOUND")


        if documents:
            sessions[index]["documents"] = documents

    return sessions


def finalize_agenda_items(session_id, agenda_items):
    for aindex, agenda_item in enumerate(agenda_items):
        if agenda_item["external_parent_id"]:
            parent = YTMigration().agendaItemGetByExtId(session_id, agenda_item["external_parent_id"])
            if parent:
                agenda_item["parent_id"] = parent["id"]
                agenda_items[aindex] = agenda_item
                YTMigration().agendaItemUpdate(agenda_item)
            else:
                print("No parent found in db for:")
                pprint(agenda_item)
    return agenda_items 


def harvest_docs():
    settings = get_settings()
    client = YTMigration().clientGet(settings["email"])
    s3 = S3Utils(
        client["s3_access_key"],
        client["s3_secret_key"],
        client["s3_bucket"])
    granicus = GranicusUtils(client, s3)

    if settings["uploaded"]:
        sessions = YTMigration().sessionsGet(client["id"], "uploaded")
    else:
        sessions = YTMigration().sessionsGet(client["id"])

    sessions = attach_documents(client, granicus, s3, settings, sessions)

    for sindex, session in enumerate(sessions):
        if 'documents' in session and session["documents"]:
            session_docs = {
                's3_key': session["s3_key"],
                'client_id': session["client_id"],
                'documents': session["documents"]}
            YTMigration().sessionUpdate(session_docs)

        if 'agenda_items' in session and session["agenda_items"]:
            for aindex, agenda_item in enumerate(session["agenda_items"]):
                agenda_item_id = YTMigration().agendaItemCreate(agenda_item)
                sessions[sindex]["agenda_items"][aindex]["id"] = agenda_item_id
            sessions[sindex]["agenda_items"] = finalize_agenda_items(session["id"], sessions[sindex]["agenda_items"])


harvest_docs()
