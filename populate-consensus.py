#!/usr/bin/python3

import sqlite3
import stem.descriptor
import os
import hashlib

DBNAME = "conhistory.sqlite"
SRCDIR = "./microdescs-2019-06"

def make_schema(cursor):
    cursor.execute("""CREATE TABLE IF NOT EXISTS history (
                           relay_id, con_published, rs_id );
                 """)
    cursor.execute("""CREATE INDEX IF NOT EXISTS i_history_ridp ON
                        history ( relay_id, con_published );""")
    cursor.execute("""CREATE INDEX IF NOT EXISTS i_history_rid ON
                        history ( relay_id );""")
    cursor.execute("""CREATE VIEW IF NOT EXISTS relays AS
                            SELECT DISTINCT relay_id from history;""")
    cursor.execute("""CREATE TABLE IF NOT EXISTS statuses (
                            rs_id UNIQUE, rs );""")
    cursor.execute("""CREATE INDEX IF NOT EXISTS i_status_rs_id ON
                        statuses ( rs_id );""")
    cursor.execute("""
                      create view if not exists consensuses as
                           select distinct con_published from history;
    """)

def add_consensus(cursor, con):
    date = str(con.valid_after)
    for fp, rs in con.routers.items():
        entry_text = rs.get_bytes()
        hash = hashlib.sha1(entry_text).hexdigest().lower()
        cursor.execute("INSERT INTO history VALUES ( ?, ?, ? )",
                       (fp.lower(), date, hash))
        cursor.execute("INSERT OR IGNORE INTO statuses VALUES ( ?, ? )",
                       (hash, entry_text))

def read_consensus(fname):
    lst = list(stem.descriptor.parse_file(open(fname, 'rb'),
                                          document_handler=stem.descriptor.DocumentHandler.DOCUMENT,
                                          validate=False))
    assert len(lst) == 1
    return lst[0]


def run():
    with sqlite3.connect(DBNAME) as connection:
        cursor = connection.cursor()
        make_schema(cursor)
        connection.commit()

        consensus_path = os.path.join(SRCDIR, "consensus-microdesc")
        for dirpath, _, filenames in os.walk(consensus_path):
            for fname in filenames:
                print(fname)
                consensus_fname = os.path.join(dirpath, fname)
                add_consensus(cursor, read_consensus(consensus_fname))

        connection.commit()

run()

