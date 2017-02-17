#!/usr/bin/python3

import sqlite3
import stem.descriptor
import os

DBNAME = "mdhistory.sqlite"
SRCDIR = "./microdescs-2017-01"

def make_schema(cursor):
    cursor.execute("""CREATE TABLE IF NOT EXISTS history (
                           relay_id, con_published, md_id );
                 """)
    cursor.execute("""CREATE INDEX IF NOT EXISTS i_history_rid ON
                        history ( relay_id, con_published );""")
    cursor.execute("""CREATE INDEX IF NOT EXISTS i_relay_id ON
                        history ( relay_id );"""
    cursor.execute("""CREATE VIEW relays AS SELECT DISTINCT relay_id from history;""")


def add_consensus(cursor, con):
    date = str(con.valid_after)
    for fp, rs in con.routers.items():
        md_digest = rs.digest
        cursor.execute("INSERT INTO history VALUES ( ?, ?, ? )",
                       (fp.lower(), date, md_digest.lower()))

def read_consensus(fname):
    lst = list(stem.descriptor.parse_file(fname,
            document_handler=stem.descriptor.DocumentHandler.DOCUMENT))
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

