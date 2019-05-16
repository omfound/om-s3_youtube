#!/usr/bin/env python3

import getopt, sys
from validate_email import validate_email
from pprint import pprint

# Migration defaults
MIGRATION_LIMIT=1


def get_settings(description):
    settings = { 
        'folder_ids': None,
        'limit': MIGRATION_LIMIT,
        'start_timestamp': None,
        'end_timestamp': None,
        'verbose': False,
        'status': None,
        'commit': False}

    # read commandline arguments, first
    fullCmdArguments = sys.argv

    # - further arguments
    argumentList = fullCmdArguments[1:]
    unixOptions = "hceflsdvt"
    gnuOptions = ["help", "commit", "email=", "folders=", "limit=", "start=", "end=", "status=", "verbose"]

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
            print ("Usage: command [CLIENT_EMAIL] [OPTION]...")
            print (description)
            print ("\nOptions:")
            print ("e, --email=EMAIL           Email address that identifies the YouTube account")
            print ("c, --commit                By default migration runs in test mode and skips permanent operations")
            print ("f, --folders=FOLDERIDS     Comma separated list of folder ids to include")
            print ("l, --limit=LIMIT           Number of harvested videos to process")
            print ("s, --start=YEAR-MM-DD      Migration will only include files after this date")
            print ("d, --end=YEAR-MM-DD        Migration will only include files before this date")
            print ("t, --status=STATUS         Only include sessions with this status")
            print ("v, --verbose               Shows additional details")
        elif currentArgument in ("-c", "--commit"):
            settings["commit"] = True
        elif currentArgument in ("-v", "--verbose"):
            settings["verbose"] = True
        elif currentArgument in ("-e", "--email"):
            if validate_email(currentValue):
                settings["email"] = currentValue
            else:
                print ("The email address provided does not appear to be valid")
                exit()
        elif currentArgument in ("-f", "--folders"):
            parsed_folder_ids = [int(e.strip()) for e in currentValue.split(',') if e.strip().isdigit()]
            if parsed_folder_ids:
                settings["folder_ids"] = parsed_folder_ids
            else:
                print ("Folder ids must be provided as a comma separated list of integers, example: 1,2,3")
                exit()
        elif currentArgument in ("-s", "--start"):
            try:
                timestamp = time.mktime(
                    datetime.datetime.strptime(
                        currentValue, "%Y-%m-%d").timetuple())
                timestamp = int(timestamp)
                settings["start_timestamp"] = timestamp
            except ValueError:
                print ("Date must be in YEAR-MM-DD format, example: 2018-01-25")
                exit()
        elif currentArgument in ("-d", "--end"):
            try:
                timestamp = time.mktime(
                    datetime.datetime.strptime(
                        currentValue, "%Y-%m-%d").timetuple())
                timestamp = int(timestamp)
                settings["end_timestamp"] = timestamp
            except ValueError:
                print ("Date must be in YEAR-MM-DD format, example: 2018-01-25")
                exit()
        elif currentArgument in ("-t", "--status"):
            if currentValue == "uploaded":
                settings["status"] = currentValue
            else:
                print ("Invalid status, valid status include: uploaded")
                exit()
        elif currentArgument in ("-l", "--limit"):
            if currentValue.strip().isdigit():
                settings["limit"] = int(currentValue.strip())
            else:
                print ("Limit must be an integer")
                exit()

    print("Using the following settings, check options with --help")
    pprint(settings)
    return settings
