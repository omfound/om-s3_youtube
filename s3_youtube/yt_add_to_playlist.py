#!/usr/bin/env python3

import os
import getopt, sys
from validate_email import validate_email

import youtube_api
from db import YTMigration

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
            print ("Add uploaded videos to their respective playlists")
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


def add_playlists():
    settings = get_settings()
    migration_client = YTMigration().clientGet(settings["email"])
    sessions = YTMigration().sessionsGet(migration_client["id"], 'uploaded')

    youtube_client = youtube_api.youtube_client_get(migration_client)

    counter = 0
    for session in sessions:
        if counter < settings["limit"]:
            properties = {
                'snippet.playlistId': session["youtube_playlist"],
                'snippet.resourceId.kind': 'youtube#video',
                'snippet.resourceId.videoId': session["youtube_id"],
                'snippet.position': ''}

            response = youtube_api.youtube_add_session_to_playlist(youtube_client, session)
            if response:
                print("Added "+session["title"]+" to playlist: "+"https://www.youtube.com/watch?v="+session["youtube_id"])
            else:
                print("Failed to add "+session["title"]+" to playlist: "+"https://www.youtube.com/watch?v="+session["youtube_id"])
            counter += 1

add_playlists()
