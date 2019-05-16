#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import flask
import json
import warnings

import google.oauth2.credentials
import google_auth_oauthlib.flow
import googleapiclient.discovery

from pprint import pprint
from s3_youtube.db import YTMigration
import io
import csv
import json
from collections import OrderedDict
from functools import wraps, update_wrapper

migration = YTMigration()
client_data = migration.clientGet("glendaleca@openmediafoundation.org")

# The CLIENT_SECRETS_FILE variable specifies the name of a file that contains
# the OAuth 2.0 information for this application, including its client_id and
# client_secret.
CLIENT_SECRETS_FILE = "/home/ubuntu/s3_youtube/client_data/"+client_data['email']+"/client_id.json"

# This OAuth 2.0 access scope allows for full read/write access to the
# authenticated user's account and requires requests to use an SSL connection.
SCOPES = ['https://www.googleapis.com/auth/youtube.force-ssl']
API_SERVICE_NAME = 'youtube'
API_VERSION = 'v3'

app = flask.Flask(__name__)
# Note: A secret key is included in the sample so that it works, but if you
# use this code in your application please replace this with a truly secret
# key. See http://flask.pocoo.org/docs/0.12/quickstart/#sessions.
app.secret_key = '6Z9MjkXoa6VrQ&y*6@p&h5*cRDm9RR'


def nocache(view):
    @wraps(view)
    def no_cache(*args, **kwargs):
        response = flask.make_response(view(*args, **kwargs))
        response.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate, post-check=0, pre-check=0, max-age=0'
        response.headers['Pragma'] = 'no-cache'
        response.headers['Expires'] = '-1'
        return response
        
    return update_wrapper(no_cache, view)


@app.route('/')
def index():
  if 'token' not in client_data or not client_data["token"]:
    return flask.redirect('authorize')

  # Load the credentials from the session.
  # client_data["token"], 
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

  client = googleapiclient.discovery.build(
      API_SERVICE_NAME, API_VERSION, credentials=credentials)
  
  return channels_list_by_username(client,
    part='snippet,contentDetails,statistics',
    forUsername='GoogleDevelopers')


@app.route('/authorize')
def authorize():
  # Create a flow instance to manage the OAuth 2.0 Authorization Grant Flow
  # steps.
  flow = google_auth_oauthlib.flow.Flow.from_client_secrets_file(
      CLIENT_SECRETS_FILE, scopes=SCOPES)
  flow.redirect_uri = flask.url_for('oauth2callback', _external=True, _scheme='https')
  authorization_url, state = flow.authorization_url(
      # This parameter enables offline access which gives your application
      # both an access and refresh token.
      access_type='offline',
      # This parameter enables incremental auth.
      include_granted_scopes='true')

  # Store the state in the session so that the callback can verify that
  # the authorization server response.
  flask.session['state'] = state

  return flask.redirect(authorization_url)


@app.route('/oauth2callback')
def oauth2callback():
  # Specify the state when creating the flow in the callback so that it can
  # verify the authorization server response.
  state = flask.session['state']
  flow = google_auth_oauthlib.flow.Flow.from_client_secrets_file(
      CLIENT_SECRETS_FILE, scopes=SCOPES, state=state)
  flow.redirect_uri = flask.url_for('oauth2callback', _external=True, _scheme='https')

  # Use the authorization server's response to fetch the OAuth 2.0 tokens.
  authorization_response = flask.request.url
  authorization_response = authorization_response.replace("http://", "https://")

  flow.fetch_token(authorization_response=authorization_response)

  credentials = flow.credentials
  
  client_refresh = {
      'email': client_data["email"],
      'token': credentials.token,
      'token_uri': credentials.token_uri,
      'client_id': credentials.client_id,
      'client_secret': credentials.client_secret,
      'scopes': credentials.scopes
  }
  if credentials.refresh_token:
      client_refresh["refresh_token"] = credentials.refresh_token

  migration.clientUpdate(client_refresh)

  return flask.redirect(flask.url_for('index'))


@app.route('/csv/<client_id>')
def sessions_csv(client_id):
    if not client_id or not client_id.isdigit():
        return("Please provide a numerical client id, e.g. /csv/1")

    sessions = migration.sessionsGet(client_id)
    keys = sessions[0].keys()

    si = io.StringIO()
    cw = csv.DictWriter(si, keys)
    cw.writeheader()
    cw.writerows(sessions)

    output = flask.make_response(si.getvalue())
    output.headers["Content-Disposition"] = "attachment; filename=export.csv"
    output.headers["Content-type"] = "text/csv"
    return(output)


@app.route('/api/sessions/<client_id>')
@nocache
def sessions_json(client_id):
    if not client_id or not client_id.isdigit():
        return("Please provide a numerical client id, e.g. /csv/1")

    session_limit = None
    session_status = None
    session_folder = None

    limit = flask.request.args.get('limit')
    if limit is not None and limit.isdigit():
        session_limit = int(limit)

    status = flask.request.args.get('status')
    if status is not None:
        if status == "harvested" or status == "uploaded":
            session_status = status

    folder = flask.request.args.get('folder')
    if folder is not None:
        if folder.isdigit():
            session_folder = folder

    count = migration.sessionsGet(client_id, session_status, session_folder, session_limit, count = True)
    sessions = migration.sessionsGet(client_id, session_status, session_folder, session_limit)

    if not count or not sessions:
        output = flask.make_response("No sessions found matching that criteria.")
        return output

    #has agenda document option
    hasAgenda = flask.request.args.get('hasAgenda')
    if hasAgenda is not None:
        agenda_sessions = []
        if hasAgenda == "true":
            for session in sessions:
                if session["documents"]:
                    for document in session["documents"]:
                        if document["type"] == "agenda" or document["type"] == "agenda_html":
                            agenda_sessions.append(session)
                            break
            sessions = agenda_sessions
        elif hasAgenda == "false":
            for session in sessions:
                sessionHasAgenda = False 
                if session["documents"]:
                    for document in session["documents"]:
                        if document["type"] == "agenda" or document["type"] == "agenda_html":
                            sessionHasAgenda = True
                if not sessionHasAgenda:
                    agenda_sessions.append(session)
            sessions = agenda_sessions

    response = api_response(sessions, count, session_limit)
    
    output = flask.make_response(json.dumps(response))
    output.headers["Content-type"] = "application/json"
    return(output)


@app.route('/api/agenda-items/<session_id>')
@nocache
def session_agenda_items_json(session_id):
    if not session_id or not session_id.isdigit():
        return("Please provide a numerical session id")

    count = migration.agendaItemsGet(session_id, limit = None, count = True)
    agendaItems = migration.agendaItemsGet(session_id, limit = None)
    response = api_response(agendaItems, count, limit = None)
    
    output = flask.make_response(json.dumps(response))
    output.headers["Content-type"] = "application/json"
    return(output)


@app.route('/api/ext-agenda-items/<client_id>/<ext_session_id>')
@nocache
def ext_session_agenda_items_json(client_id, ext_session_id):
    if not ext_session_id or not ext_session_id.isdigit():
        return("Please provide a numerical external session id")

    count = migration.agendaItemsGetByExtId(client_id, ext_session_id, limit = None, count = True)
    agendaItems = migration.agendaItemsGetByExtId(client_id, ext_session_id, limit = None)
    response = api_response(agendaItems, count, limit = None)
    
    output = flask.make_response(json.dumps(response))
    output.headers["Content-type"] = "application/json"
    return(output)
    

def api_response(results, count, limit = None):
    if results:
        response = OrderedDict([
            ('limit', limit),
            ('start', 0),
            ('totalSize', count),
            ('size', len(results)),
            ('results', results)])
    else:
        response = OrderedDict([
            ('limit', limit),
            ('start', 0),
            ('totalSize', 0),
            ('size', 0),
            ('results', None)])

    return response
        

def channels_list_by_username(client, **kwargs):
  response = client.channels().list(
    **kwargs
  ).execute()

  return flask.jsonify(**response)
