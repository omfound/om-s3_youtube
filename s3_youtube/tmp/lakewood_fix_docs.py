#!/usr/bin/env python3

import boto3
import botocore 
from pprint import pprint
from db import YTMigration
from s3_api_utils import S3Utils
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
            print ("Usage: s3_lakewood_harvest [CLIENT_EMAIL] [OPTION]...")
            print ("Harvest video information from a Granicus S3 export")
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


def parse_video_key(key):
    filename = key.replace("Videos/", "")
    filename_parts = filename.split("-")

    parts = {
        's3_key': key,
        's3_filename': filename,
        'title': None,
        'original_title': None,
        'session_id': None,
        'date': None,
        'session_date': None,
        'session_timestamp': None,
        'session_folder': None,
        'youtube_playlist':'PLrCjxOQpsX7I0lW6aMQI9gGti1J3VZXO_',
        'category': "City Council",
        'session_description': None}


    try:
        video_id = int(filename_parts[0])
        del(filename_parts[0])
    except ValueError:
        return False

    date_index = None
    for index,part in enumerate(filename_parts):
        #071618
        if len(part) == 6:
            try:
                date = int(part)
                date_index = index
                parts["date"] = part.strip()

                datemanip = datetime.datetime.strptime(parts["date"], "%m%d%y")
                strdate = str(datemanip.date())

                parts["session_date"] = strdate+"T"+"12:00:00-07:00"
                pydate = dateutil.parser.parse(parts["session_date"])
                parts["session_timestamp"] = int(pydate.timestamp())
            except ValueError:
                continue

    title_parts = []
    if date_index:
        for index, part in enumerate(filename_parts):
            if index >= date_index:
              break
            if part:
                title_parts.append(part)
    if title_parts:
        title = ' '.join(title_parts)
        parts["original_title"] = title.strip()

    if date_index:
        parts["title"] = pydate.strftime("%-m-%-d-%y")+" Council Meeting"

    return parts


def sessions_store(settings, migration_client, sessions):
    counter = 0
    for session in sessions:
        if counter < settings["limit"]:
            existing_session = YTMigration().sessionGet(migration_client["id"], session)
            if not existing_session:
                YTMigration().sessionStore(migration_client["id"], session)
                counter += 1
    return


def prepare_sessions(s3):
    #"Agendas/01 AGENDA 2017 CITY COUNCIL JOINT STUDY SESSION PACKET 09 18.pdf"
    #"Videos/722-Study-Session-030518-Medium-v1.mp4"
    sessions = []

    videos = s3.filesGet(prefix="Videos",suffix=".mp4")
    for video in videos:
        parts = parse_video_key(video)
        if parts["title"]:
            sessions.append(parts)
        else:
            print("Failed to parse: "+video)

    return sessions 


def parse_agenda_key(key):
    parts = {
        'key': key,
        'year': None,
        'month': None,
        'day': None,
        'session_date': None,
        'session_timestamp': None
    }

    filename = key.replace("Agendas/", "")
    parts["filename"] = filename

    filename = filename.replace(".pdf", "")
    filename_parts = filename.split(" ")

    if filename_parts[2].isdigit():
        parts["year"] = filename_parts[2]

    day_key = len(filename_parts) - 1
    if filename_parts[day_key].isdigit() and len(filename_parts[day_key]) == 2:
        parts["day"] = filename_parts[day_key]

    month_key = len(filename_parts) - 2
    if filename_parts[month_key].isdigit() and len(filename_parts[month_key]) == 2:
        parts["month"] = filename_parts[month_key]

    if parts["year"] and parts["month"] and parts["day"]:
        parts["session_date"] = parts["year"]+"-"+parts["month"]+"-"+parts["day"]+"T"+"12:00:00-07:00"
        pydate = dateutil.parser.parse(parts["session_date"])
        parts["session_timestamp"] = int(pydate.timestamp())
	
    return parts


def attach_agendas(s3, sessions):
    agendas = s3.filesGet(prefix="Agendas",suffix=".pdf")
    for agenda in agendas:
        agenda_parts = parse_agenda_key(agenda)
        for index,session in enumerate(sessions):
            if session["session_timestamp"] and agenda_parts["session_timestamp"]:
                if session["session_timestamp"] == agenda_parts["session_timestamp"]:
                    documents = []
                    documents.append({
                        "type": "agenda",
                        "location": "internal",
                        "s3_key": agenda_parts["key"],
                        "filename": agenda_parts["filename"]})
                    sessions[index]["documents"] = documents

    return sessions


def fix():
    settings = get_settings()
    migration_client = YTMigration().clientGet(settings["email"])
    s3 = S3Utils(
        migration_client["s3_access_key"],
        migration_client["s3_secret_key"],
        migration_client["s3_bucket"])
    sessions = YTMigration().sessionsGet(migration_client["id"])
    sessions = attach_agendas(s3, sessions)

    for session in sessions:
        if 'documents' in session and session["documents"]:
            session_docs = {
                's3_key': session["s3_key"],
                'client_id': session["client_id"],
                'documents': session["documents"]}
            YTMigration().sessionUpdate(session_docs)


fix()
