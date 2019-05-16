#!/usr/bin/env python3

import os
import getopt, sys
from validate_email import validate_email
from pprint import pprint

from db import YTMigration
from s3_api_utils import S3Utils
from granicus_api import GranicusUtils


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
        print ("Email address for the account must be provided")
        exit()
    for currentArgument, currentValue in arguments:  
        if currentArgument in ("-h", "--help"):
            print ("Usage: granicus_harvest_html_agendas.py [CLIENT_EMAIL]")
            print ("Harvest document information from a Granicus S3 export")
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


def harvest_html_agendas():
    settings = get_settings()

    client = YTMigration().clientGet(settings["email"])
    s3 = S3Utils(
        client["s3_access_key"],
        client["s3_secret_key"],
        client["s3_bucket"])
    granicus = GranicusUtils(client, s3)
    sessions = YTMigration().sessionsGet(client["id"])
    
    for session in sessions:
        agenda_html = granicus.htmlAgendaTransform(session["session_id"])
        if agenda_html:
            storage_folder = "/nvme/client_files/"+client["email"]
            agenda_folder = storage_folder+"/documents/agenda_html"
            agenda_filename = session["session_id"]+"_agenda.html"
            agenda_path = agenda_folder+"/"+agenda_filename
            agenda_url = "https://s3-youtube.open.media/client_files/"+client["email"]+"/documents/agenda_html/"+agenda_filename

            os.makedirs(agenda_folder, exist_ok=True)
            html_file = open(agenda_path,"w")
            html_file.write(agenda_html)
            html_file.close()

            if not session["documents"]:
                session["documents"] = []

            session["documents"].append({
                "type": "agenda_html",
                "location": "internal",
                "s3_key": None,
                "filename": agenda_filename,
                "path": agenda_path,
                "url": agenda_url})

            session_docs = {
                's3_key': session["s3_key"],
                'client_id': session["client_id"],
                'documents': session["documents"]}
            YTMigration().sessionUpdate(session_docs)
            print("Harvested: "+agenda_path)


harvest_html_agendas()
