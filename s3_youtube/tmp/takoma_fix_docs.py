#!/usr/bin/env python3

from db import YTMigration
from s3_api_utils import S3Utils

from pprint import pprint

def fix_tacoma_docs():
    migration_client = YTMigration().clientGet("takomapark@openmediafoundation.org")
    s3 = S3Utils(
        migration_client["s3_access_key"],
        migration_client["s3_secret_key"],
        migration_client["s3_bucket"])

    sessions = YTMigration().sessionsGet(migration_client["id"], 'uploaded')

    for session in sessions:
        if session["documents"]:
            docUpdate = False
            for index, document in enumerate(session["documents"]):
                if "filename" in document and document["filename"]:
                    ext_agenda_item = YTMigration().agendaItemGetBySessionFilename(session["id"], document["filename"])
                    if ext_agenda_item:
                        session["documents"][index]["external_agenda_item_id"] = ext_agenda_item["id"]
                        docUpdate = True
            
            if docUpdate:
                session_docs = {
                    's3_key': session["s3_key"],
                    'client_id': session["client_id"],
                    'documents': session["documents"]}
                pprint(session_docs)
                #YTMigration().sessionUpdate(session_docs)

fix_tacoma_docs()
