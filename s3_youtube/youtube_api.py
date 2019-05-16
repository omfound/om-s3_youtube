#!/usr/bin/env python3

import http.client
import httplib2
from dateutil import parser
from pytz import timezone

#google modules
import google.oauth2.credentials
import googleapiclient.discovery
import google_auth_oauthlib.flow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google_auth_oauthlib.flow import InstalledAppFlow
from apiclient.http import MediaFileUpload

# YouTube API service settings
API_SERVICE_NAME = 'youtube'
API_VERSION = 'v3'

# Explicitly tell the underlying HTTP transport library not to retry, since
# we are handling retry logic ourselves.
httplib2.RETRIES = 1

# Maximum number of times to retry before giving up on upload.
MAX_RETRIES = 10

# Always retry when these exceptions are raised.
RETRIABLE_EXCEPTIONS = (httplib2.HttpLib2Error, IOError, http.client.NotConnected,
  http.client.IncompleteRead, http.client.ImproperConnectionState,
  http.client.CannotSendRequest, http.client.CannotSendHeader,
  http.client.ResponseNotReady, http.client.BadStatusLine)

# Always retry when an apiclient.errors.HttpError with one of these status
# codes is raised.
RETRIABLE_STATUS_CODES = [500, 502, 503, 504]

def youtube_client_get(client_data):
    # Load the credentials from the session.
    credentials = google.oauth2.credentials.Credentials(
        client_data["token"], 
        client_data["refresh_token"], 
        None,
        client_data["token_uri"], 
        client_data["client_id"], 
        client_data["client_secret"], 
        client_data["scopes"])

    if credentials.token != client_data["token"]:
        client_data["token"] = credentials.token
        migration.clientUpdate(client_data)

    client = googleapiclient.discovery.build(API_SERVICE_NAME, API_VERSION, credentials=credentials)
    return client


# This method implements an exponential backoff strategy to resume a
# failed upload.
def youtube_resumable_upload(properties, request, resource, method):
    response = None
    error = None
    retry = 0
    while response is None:
        try:
            message = "Uploading: "+properties["snippet.title"]
            print(message)
            status, response = request.next_chunk()
            if response is not None:
                if method == 'insert' and 'id' in response:
                    return response
                elif method != 'insert' or 'id' not in response:
                    print(response)
                    return false
                else:
                    print(response)
                    return false
        except HttpError as e:
            if e.resp.status in RETRIABLE_STATUS_CODES:
                error = "A retriable HTTP error %d occurred:\n%s" % (e.resp.status,e.content)
            else:
                print(e.content)
                raise
        except RETRIABLE_EXCEPTIONS as e:
            error = "A retriable error occurred: %s" % e

        if error is not None:
            print(error)
            retry += 1
            if retry > MAX_RETRIES:
                exit("No longer attempting to retry.")

            max_sleep = 2 ** retry
            sleep_seconds = random.random() * max_sleep
            print("Sleeping %f seconds and then retrying...")
            time.sleep(sleep_seconds)


# Remove keyword arguments that are not set
def youtube_remove_empty_kwargs(**kwargs):
    good_kwargs = {}
    if kwargs is not None:
        for key, value in kwargs.items():
            if value:
                good_kwargs[key] = value
    return good_kwargs


# Build a resource based on a list of properties given as key-value pairs.
# Leave properties with empty values out of the inserted resource.
def youtube_build_resource(properties):
    resource = {}
    for p in properties:
        # Given a key like "snippet.title", split into "snippet" and "title", where
        # "snippet" will be an object and "title" will be a property in that object.
        prop_array = p.split('.')
        ref = resource
        for pa in range(0, len(prop_array)):
            is_array = False
            key = prop_array[pa]

            # For properties that have array values, convert a name like
            # "snippet.tags[]" to snippet.tags, and set a flag to handle
            # the value as an array.
            if key[-2:] == '[]':
                key = key[0:len(key)-2:]
                is_array = True

            if pa == (len(prop_array) - 1):
                # Leave properties without values out of inserted resource.
                if properties[p]:
                    if is_array:
                        ref[key] = properties[p].split(',')
                    else:
                        ref[key] = properties[p]
            elif key not in ref:
                # For example, the property is "snippet.title", but the resource does
                # not yet have a "snippet" object. Create the snippet object here.
                # Setting "ref = ref[key]" means that in the next time through the
                # "for pa in range ..." loop, we will be setting a property in the
                # resource's "snippet" object.
                ref[key] = {}
                ref = ref[key]
            else:
                # For example, the property is "snippet.description", and the resource
                # already has a "snippet" object.
                ref = ref[key]
    return resource


def youtube_video_insert(client, properties, media_file, **kwargs):
    resource = youtube_build_resource(properties) # See full sample for function
    kwargs = youtube_remove_empty_kwargs(**kwargs) # See full sample for function
    request = client.videos().insert(
        body=resource,
        media_body=MediaFileUpload(media_file, chunksize=-1,resumable=True),
        **kwargs)

    return youtube_resumable_upload(properties, request, 'video', 'insert')


def youtube_playlist_items_insert(youtube_client, properties, **kwargs):
    # See fulddl sample for function
    resource = youtube_build_resource(properties)

    # See full sample for function
    kwargs = youtube_remove_empty_kwargs(**kwargs)

    try:
        response = youtube_client.playlistItems().insert(body=resource,**kwargs).execute()
    except HttpError as e:
        print("Failed to add video to playlist")
        print(e)
        return False

    return response


def youtube_add_session_to_playlist(youtube_client, session):
    properties = {
        'snippet.playlistId': session["youtube_playlist"],
        'snippet.resourceId.kind': 'youtube#video',
        'snippet.resourceId.videoId': session["youtube_id"],
        'snippet.position': ''}

    response = youtube_playlist_items_insert(youtube_client, properties, part='snippet')
    return response


def youtube_upload_session(youtube_client, session):
    media_file = session["local_path"]
    if session["session_date"]:
        datetime = parser.parse(session["session_date"])
        utcDate = datetime.astimezone(timezone('UTC'))
        youtubeDate = utcDate.replace(microsecond=0).isoformat("T")
        recordingDate = youtubeDate.replace('+00:00', '.0Z')
    else:
        recordingDate = None

    properties = {
        'snippet.title': session["title"],
        'snippet.defaultLanguage': '',
        'snippet.categoryId': 25,
        'status.privacyStatus': 'public',
        'status.license': '',
        'status.embeddable': '',
        'snippet.description': '',
        'recordingDetails.recordingDate': recordingDate}

    if session["category"]:
        properties["snippet.tags"] = [session["category"]]

    return youtube_video_insert(youtube_client, properties, media_file, part='snippet,status,recordingDetails')
