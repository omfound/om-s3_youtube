#!/usr/bin/env python3

import uuid
import csv
import os
import json
import requests
import urllib.parse as parse
from db import YTMigration as db
from pprint import pprint

from bs4 import BeautifulSoup

from s3_api_utils import S3Utils

STORAGE_PATH = "/nvme/client_files/"

class GranicusUtils():
    def __init__(self, client, s3):
        self.client = client
        self.s3 = s3
        self.db = db()


    def foldersGet(self):
        folders = []

        folders = self.s3.foldersGet(self.client["s3_prefix"])

        if folders:
            return folders
        else:
            return None


    def documentsGet(self, folder):
        documents = []

        metadata_files = self.s3.filesGet(folder)
        exclusions = [".csv", "agenda_url", ".mp4", ".mp3", ".wmv"]
        if metadata_files:
            for mfile in metadata_files:
                if not any(x in mfile for x in exclusions):
                    documents.append(mfile)
        
        if documents:
            return documents
        else:
            return False

    
    def metadataGet(self, folder, mtype="archive"):
        download_path = STORAGE_PATH+self.client["email"]+"/files/"+mtype+".csv"

        metadata_files = self.s3.filesGet(folder, mtype+".csv")

        if metadata_files:
            self.s3.fileDownload(metadata_files[0], download_path)
            
            with open(download_path, "r") as f:
                #agenda csv has no header
                if mtype == "agenda":
                    reader = csv.DictReader(f, self.agendaCSVHeader())
                elif mtype == "document":
                    reader = csv.DictReader(f, self.documentCSVHeader())
                else:
                    reader = csv.DictReader(f)
                metadata = list(reader)

            os.remove(download_path)
            
            if metadata:
                return metadata
        return False


    def htmlTransformLinks(self, html):
        soup = BeautifulSoup(html, features="html.parser")

        if not soup or str(soup) == "Not found." or str(soup) == "Page not found.":
            return None

        #need to replace document and video cuepoint links
        for link in soup.find_all('a'):
            href = link.get("href")
            if href:
                link['target'] = "_parent"
                params = parse.parse_qs(parse.urlsplit(href).query)
                if "meta_id" in params and params["meta_id"]:
                    if "clip_id" in params and params["clip_id"]:
                        session = self.db.sessionGetByExtId(self.client["id"], params["clip_id"][0])
                        agenda_item = self.db.agendaItemGetByExtId(session["id"], params["meta_id"][0])
                        if session and agenda_item:
                            link['href'] = "https://"+self.client["granicus_id"]+".open.media/external-redirect"
                            link['href'] += "/"+str(agenda_item["external_session_id"])
                            link['href'] += "/"+str(agenda_item["external_id"])
                        elif not session:
                            print("No session found for: "+params["clip_id"][0])
                        elif not agenda_item:
                            print("No agenda item found for: "+params["clip_id"][0])
        return str(soup.prettify(formatter="html"))


    def htmlAgendaTransform(self, id, view_id=14):
        #https://surpriseaz.open.media/sessions/27529?embedInPoint=74
        url = "https://"+self.client["granicus_id"]+".granicus.com/GeneratedAgendaViewer.php?view_id="+str(view_id)+"&clip_id="+str(id)
        r = requests.get(url, allow_redirects=True)
        html = r.content

        agenda = self.htmlTransformLinks(html)
        return agenda


    def htmlMinutesTransform(self, clip_id, doc_id, view_id=14):
        #http://takomapark.granicus.com/MinutesViewer.php?view_id=14&clip_id=921&doc_id=132d587b-d629-102f-b6fc-79f14fb49beb
        url = "https://"+self.client["granicus_id"]+".granicus.com/MinutesViewer.php?view_id="+str(view_id)+"&clip_id="+str(clip_id)+"&doc_id="+doc_id
        print("Fetching: "+url)
        r = requests.get(url, allow_redirects=True)
        html = r.content

        minutes = self.htmlTransformLinks(html)
        return minutes 


    def apiFetchObject(self, id):
        url = "http://search.granicus.com/api/"+self.client["granicus_id"]+".granicus.com/_search"

        data = json.dumps({
            "query": {
                "terms": {
                    "_id": [id]
                }
            }
        })

        response = requests.post(url, data)
        gobject = response.json()

        if gobject and 'hits' in gobject and gobject['hits']['total'] >= 1:
            return gobject["hits"]["hits"][0]["_source"]
        else:
            return None


    def agendaCSVHeader(self):
        return (
            "id", 
            "session_id", 
            "parent_item_id", 
            "type", 
            "start_seconds", 
            "label", 
            "attachment_label",
            "unknown2",
            "attachment_filename",
            "unknown4",
            "unknown5",
            "unknown6",
            "unknown7",
            "date")


    def documentCSVHeader(self):
        return (
            "id",
            "id2",
            "label",
            "status",
            "filename",
            "filename2",
            "unknown1",
            "unknown2",
            "unknown3",
            "unknown4",
            "unknown5",
            "unknown6",
            "unknown7",
            "hidden",
            "published",
            "unknown10")


    def keyFolderGet(self, key):
        key_parts = key.split("/")
        filename = key_parts[-1]
        folder = key.replace(filename, "")
        return folder

