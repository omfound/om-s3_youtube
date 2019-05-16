#!/usr/bin/env python3

import MySQLdb
import configparser
from os import path
from pprint import pprint
import time
import datetime
import dateutil.parser
import pickle


class YTMigration():

    def __init__(self):
        config_path = path.join(path.abspath(path.dirname(__file__)), '..', 'config.ini')
        config = configparser.ConfigParser()
        config.read(config_path)

        db = MySQLdb.connect(
            host=config["mysql"]["host"], 
            user=config["s3_youtube_db"]["user"], 
            passwd=config["s3_youtube_db"]["passwd"],
            db=config["s3_youtube_db"]["db"],
            charset='utf8',
            use_unicode=True)
        
        self.db = db
        self.cursor = db.cursor()


    def clientGet(self, email):
        select_stmt = """
            SELECT *
            FROM clients
            WHERE email = %s
            """
        data = (email, )
        self.cursor.execute(select_stmt, data)
        result = self.cursor.fetchone()
        self.db.commit()
        
        if result:
            client = self.mapClient(result)
            return client 
        else:
            return False
    

    def clientStore(self, client):
        insert_stmt = (
            """INSERT INTO clients 
            (email, label, token, refresh_token, token_uri, client_id, 
            client_secret, scopes, s3_access_key, s3_secret_key, s3_prefix, s3_bucket, granicus_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
        )

        data = (
            client["email"],
            client["label"],
            client["token"],
            client["refresh_token"],
            client["token_uri"],
            client["client_id"],
            client["client_secret"],
            client["scopes"],
            client["s3_access_key"],
            client["s3_secret_key"],
            client["s3_prefix"],
            client["s3_bucket"],
            client["granicus_id"])

        self.cursor.execute(insert_stmt, data)
        insert_id = self.db.insert_id()
        self.db.commit()
        return insert_id

    def clientUpdate(self, client):
        updated_client = self.clientGet(client["email"])
        updated_client.update(client)

        update_stmt = """
            UPDATE clients 
            SET label = %s, token = %s, refresh_token = %s, token_uri = %s, 
            client_id = %s, client_secret = %s, scopes = %s
            WHERE email=%s"""
            
        data = (
            updated_client["label"],
            updated_client["token"],
            updated_client["refresh_token"],
            updated_client["token_uri"],
            updated_client["client_id"],
            updated_client["client_secret"],
            updated_client["scopes"],
            updated_client["email"])

        self.cursor.execute(update_stmt, data)
        self.db.commit()
        return updated_client


    def mapClient(self, result):
        client = {
            'id': result[0],
            'email': result[1],
            'label': result[2],
            'token': result[3],
            'refresh_token': result[4],
            'token_uri': result[5],
            'client_id': result[6],
            'client_secret': result[7],
            'scopes': result[8],
            's3_access_key': result[9],
            's3_secret_key': result[10],
            's3_prefix': result[11],
            's3_bucket': result[12],
            'granicus_id': result[13]}
        return client 

    def sessionStore(self, client_id, session):
        if 'documents' not in session or not session["documents"]:
            insert_stmt = (
                """INSERT INTO sessions 
                (client_id, title, s3_key, s3_filename, documents, session_id, 
                session_date, session_timestamp, session_folder, session_description, 
                category, youtube_playlist, status) 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""")
        else:
            insert_stmt = (
                """INSERT INTO sessions 
                (client_id, title, s3_key, s3_filename, documents, session_id, 
                session_date, session_timestamp, session_folder, session_description, 
                category, youtube_playlist, status) 
                VALUES (%s, %s, %s, %s, _binary %s, %s, %s, %s, %s, %s, %s, %s, %s)""")

        if 'category' not in session:
            session["category"] = None

        if 'youtube_playlist' not in session:
            session["youtube_playlist"] = None

        if 'documents' not in session:
            session["documents"] = None 
        elif session["documents"]:
            session["documents"] = pickle.dumps(session["documents"])

        if 'status' not in session:
            session["status"] = 'harvested'

        if 'session_id' not in session:
            session["session_id"] = None

        if 'session_folder' not in session:
            session["session_folder"] = None

        if 'session_timestamp' not in session:
            session["session_timestamp"] = None

        if 'session_date' not in session:
            session["session_date"] = None
        elif session["session_date"]:
            pydate = dateutil.parser.parse(session["session_date"])
            session["session_timestamp"] = pydate.timestamp()

        if 'session_description' not in session:
            session["session_description"] = None

        data = (
            client_id,
            session["title"],
            session["s3_key"],
            session["s3_filename"],
            session["documents"],
            session["session_id"],
            session["session_date"],
            session["session_timestamp"],
            session["session_folder"],
            session["session_description"],
            session["category"],
            session["youtube_playlist"],
            session["status"])

        try:
            self.cursor.execute(insert_stmt, data)
            insert_id = self.db.insert_id()
            self.db.commit()
        except:
            print(self.cursor._last_executed)
            exit()
        return insert_id


    def sessionGet(self, client_id, session):
        select_stmt = """
            SELECT *
            FROM sessions 
            WHERE client_id = %s AND s3_key = %s
            """
        data = (client_id, session["s3_key"])
        self.cursor.execute(select_stmt, data)
        result = self.cursor.fetchone()
        self.db.commit()
        
        if result:
            session = self.mapSession(result)
            return session 
        else:
            return False


    def sessionGetByExtId(self, client_id, ext_id):
        select_stmt = """
            SELECT *
            FROM sessions
            WHERE client_id = %s AND session_id = %s
            """

        data = (client_id, ext_id)
        self.cursor.execute(select_stmt, data)
        result = self.cursor.fetchone()
        self.db.commit()
        
        if result:
            session = self.mapSession(result)
            return session 
        else:
            return None 

    
    def sessionsGet(self, client_id, status = None, folder = None, limit = None, count = False):
        sessions = [] 

        select_stmt = """
            SELECT *
            FROM sessions 
            WHERE client_id = %s
            """

        count_stmt = """
            SELECT COUNT(*)
            FROM sessions
            WHERE client_id = %s
            """

        data_list = [client_id]
        count_list = [client_id]
        
        if status:
            select_stmt += " AND status = %s"
            count_stmt += " AND status = %s"
            data_list.append(status)
            count_list.append(status)
        if folder:
            select_stmt += " AND session_folder = %s"
            count_stmt += " AND session_folder = %s"
            data_list.append(folder)
            count_list.append(folder)
        if limit:
            select_stmt += " LIMIT %s"
            data_list.append(limit)
        
        data = tuple(data_list)
        count_data = tuple(count_list)
        if not count:
            self.cursor.execute(select_stmt, data)
            self.db.commit()
        
            for result in self.cursor:
                sessions.append(self.mapSession(result))

            if len(sessions) > 0:
                return sessions 
            else:
                return None 
        else:
            self.cursor.execute(count_stmt, count_data)
            result = self.cursor.fetchone()
            self.db.commit()
            return result[0]


    def sessionUpdate(self, session):
        updated_session = self.sessionGet(session["client_id"], session)
        updated_session.update(session)

        if updated_session["documents"]:
            update_stmt = """
                UPDATE sessions 
                SET title = %s, s3_filename = %s, documents = _binary %s, session_id = %s, 
                session_date = %s, session_timestamp = %s, session_folder = %s, 
                session_description = %s, category = %s, youtube_playlist = %s, 
                youtube_id = %s, status = %s 
                WHERE client_id = %s AND s3_key = %s"""
        else:
            updated_session["documents"] = None
            update_stmt = """
                UPDATE sessions 
                SET title = %s, s3_filename = %s, documents = %s, session_id = %s, 
                session_date = %s, session_timestamp = %s, session_folder = %s, 
                session_description = %s, category = %s, youtube_playlist = %s, 
                youtube_id = %s, status = %s 
                WHERE client_id = %s AND s3_key = %s"""

        if updated_session["session_date"] and not updated_session["session_timestamp"]:
            pydate = dateutil.parser.parse(updated_session["session_date"])
            updated_session["session_timestamp"] = pydate.timestamp()

        if "documents" in updated_session and updated_session["documents"]:
            updated_session["documents"] = pickle.dumps(updated_session["documents"])
            
        data = (
            updated_session["title"],
            updated_session["s3_filename"],
            updated_session["documents"],
            updated_session["session_id"],
            updated_session["session_date"],
            updated_session["session_timestamp"],
            updated_session["session_folder"],
            updated_session["session_description"],
            updated_session["category"],
            updated_session["youtube_playlist"],
            updated_session["youtube_id"],
            updated_session["status"],
            updated_session["client_id"],
            updated_session["s3_key"])

        self.cursor.execute(update_stmt, data)
        self.db.commit()
        return updated_session


    def agendaItemsGet(self, session_id, limit = None, count = False):
        agendaItems = []

        select_stmt = """
            SELECT *
            FROM agenda_items 
            WHERE session_id = %s
            """

        count_stmt = """
            SELECT COUNT(*)
            FROM agenda_items
            WHERE session_id = %s
            """

        data = (session_id, )
        count_data = (session_id, )
        
        if limit:
            select_stmt += " LIMIT %s"
            data = (client_id, limit)
        
        if not count:
            self.cursor.execute(select_stmt, data)
            self.db.commit()
        
            for result in self.cursor:
                agendaItems.append(self.mapAgendaItem(result))

            if len(agendaItems) > 0:
                return agendaItems 
            else:
                return None 
        else:
            self.cursor.execute(count_stmt, count_data)
            result = self.cursor.fetchone()
            self.db.commit()
            return result[0]


    def agendaItemsGetByExtId(self, client_id, ext_session_id, limit = None, count = False):
        agendaItems = []

        select_stmt = """
            SELECT *
            FROM agenda_items 
            JOIN sessions ON agenda_items.session_id = sessions.id
            WHERE external_session_id = %s AND sessions.client_id = %s
            """

        count_stmt = """
            SELECT COUNT(*)
            FROM agenda_items
            JOIN sessions ON agenda_items.session_id = sessions.id
            WHERE external_session_id = %s AND sessions.client_id = %s
            """

        data = (ext_session_id,client_id)
        count_data = (ext_session_id,client_id)
        
        if limit:
            select_stmt += " LIMIT %s"
            data = (ext_session_id,client_id,limit)
            count_data = (ext_session_id,client_id)
        
        if not count:
            self.cursor.execute(select_stmt, data)
            self.db.commit()
        
            for result in self.cursor:
                agendaItems.append(self.mapAgendaItem(result))

            if len(agendaItems) > 0:
                return agendaItems 
            else:
                return None 
        else:
            self.cursor.execute(count_stmt, count_data)
            result = self.cursor.fetchone()
            self.db.commit()
            return result[0]


    def agendaItemGet(self, session_id, id):
        select_stmt = """
            SELECT *
            FROM agenda_items 
            WHERE id = %s AND session_id = %s
            """

        data = (id, session_id)
        self.cursor.execute(select_stmt, data)
        result = self.cursor.fetchone()
        self.db.commit()
        
        if result:
            agendaItem = self.mapAgendaItem(result)
            return agendaItem 
        else:
            return None 

    
    def agendaItemGetBySessionFilename(self, session_id, filename):
        select_stmt = """
            SELECT *
            FROM agenda_items 
            WHERE session_id = %s AND attachment_filename = %s
            """

        data = (session_id, filename)
        self.cursor.execute(select_stmt, data)
        result = self.cursor.fetchone()
        self.db.commit()
        
        if result:
            agendaItem = self.mapAgendaItem(result)
            return agendaItem 
        else:
            return None 
    

    def agendaItemGetByExtId(self, session_id, ext_id):
        select_stmt = """
            SELECT *
            FROM agenda_items 
            WHERE external_id = %s AND session_id = %s
            """

        data = (ext_id, session_id)
        self.cursor.execute(select_stmt, data)
        result = self.cursor.fetchone()
        self.db.commit()
        
        if result:
            agendaItem = self.mapAgendaItem(result)
            return agendaItem 
        else:
            return None 


    def agendaItemCreate(self, agenda_item):
        insert_stmt = (
            """INSERT INTO agenda_items 
            (parent_id, session_id, external_id, external_session_id, 
            external_parent_id, label, end, start, attachment_label, 
            attachment_filename, type)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
        )

        data = (
            agenda_item["parent_id"],
            agenda_item["session_id"],
            agenda_item["external_id"],
            agenda_item["external_session_id"],
            agenda_item["external_parent_id"],
            agenda_item["label"],
            agenda_item["end"],
            agenda_item["start"],
            agenda_item["attachment_label"],
            agenda_item["attachment_filename"],
            agenda_item["type"])
        
        self.cursor.execute(insert_stmt, data)
        insert_id = self.db.insert_id()
        self.db.commit()
        return insert_id


    def agendaItemUpdate(self, agenda_item):
        merged_agenda_item = self.agendaItemGet(agenda_item["session_id"], agenda_item["id"])
        merged_agenda_item.update(agenda_item)

        update_stmt = """
            UPDATE agenda_items 
            SET parent_id = %s, session_id = %s, external_session_id = %s, 
            external_id = %s, external_parent_id = %s, label = %s, end = %s, 
            start = %s, attachment_label = %s, attachment_filename = %s, 
            type = %s
            WHERE id = %s"""

        data = (
            merged_agenda_item["parent_id"],
            merged_agenda_item["session_id"],
            merged_agenda_item["external_session_id"],
            merged_agenda_item["external_id"],
            merged_agenda_item["external_parent_id"],
            merged_agenda_item["label"],
            merged_agenda_item["end"],
            merged_agenda_item["start"],
            merged_agenda_item["attachment_label"],
            merged_agenda_item["attachment_filename"],
            merged_agenda_item["type"],
            agenda_item["id"])

        return merged_agenda_item 


    def mapSession(self, result):
        if result[5]:
            documents = pickle.loads(result[5])
        else:
            documents = result[5]

        session = {
            'id': result[0],
            'client_id': result[1],
            'title': result[2],
            's3_key': result[3],
            's3_filename': result[4],
            'documents': documents,
            'session_id': result[6],
            'session_date': result[7],
            'session_timestamp': result[8],
            'session_folder': result[9],
            'session_description': result[10],
            'category': result[11],
            'youtube_playlist': result[12],
            'youtube_id': result[13],
            'status': result[14]}
        return session

    def mapAgendaItem(self, result):
        agenda_item = {
            'id': result[0],
            'parent_id': result[1],
            'session_id': result[2],
            'external_session_id': result[3],
            'external_id': result[4],
            'external_parent_id': result[5],
            'label': result[6],
            'end': result[7],
            'start': result[8],
            'attachment_label': result[9],
            'attachment_filename': result[10],
            'type': result[11]}
        return agenda_item
