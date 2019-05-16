#!/usr/bin/env python3

from db import YTMigration
from pprint import pprint

def finalize_agenda_items(session_id, agenda_items):
    for aindex, agenda_item in enumerate(agenda_items):
        if agenda_item["external_parent_id"]:
            parent = YTMigration().agendaItemGetByExtId(session_id, agenda_item["external_parent_id"])
            agenda_item["parent_id"] = parent["id"]
            agenda_items[aindex] = agenda_item
            YTMigration().agendaItemUpdate(agenda_item)
    return agenda_items 

def debugit():
    client = YTMigration().clientGet("takomapark@openmediafoundation.org")
    session_id = 102
    agenda_items = YTMigration().agendaItemsGet(session_id)
    finalize_agenda_items(session_id, agenda_items)

debugit()
