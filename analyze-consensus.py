#!/usr/bin/python3

import sqlite3
import os
import stem.descriptor
import itertools
DBNAME = "conhistory.sqlite"
SRCDIR = "./microdescs-2017-01"

def find_changes(rs1, rs2):
    if rs1 == rs2 == None:
        return ""
    elif rs1 == None:
        return "+*"
    elif rs2 == None:
        return "-*"

    differences = []
    for letter, attr in [('n', 'nickname'),
                         ('i', 'fingerprint'),
                         ('a', 'address'),
                         ('d', 'published'),
                         ('p', 'or_port'),
                         ('P', 'dir_port'),
                         ('F', 'flags'),
                         ('v', 'version_line'),
                         ('V', 'protocols'),
                         ('w', 'bandwidth'),
                         ('W', 'measured'),
                         ('u', 'unrecognized_bandwidth_entries'),
                         ('M', 'digest')]:
        if getattr(rs1, attr) != getattr(rs2, attr):
            differences.append(letter)
    return "".join(differences)

def find_differences(rslist):
    result = []
    prev_rs = None
    for rs in rslist:
        ch = find_changes(prev_rs, rs)
        if ch:
            result.append(ch)
        prev_rs = rs
    return result

CONSENSUSES = { }

def analyze_relay(fp, connection):
    cur = connection.cursor()
    cur.execute("""select count(*) from history where relay_id = ?;""", (fp,))
    row = cur.fetchone()
    n_consensuses = row[0]

    cur.execute("""select rs_id, count(*) from history where relay_id = ?
                   group by rs_id order by con_published;""", (fp,))
    rows = cur.fetchall()
    n_mds = len(rows)

    cur.execute("""select rs, con_published from history, statuses where relay_id = ? and history.rs_id == statuses.rs_id
                   order by con_published;""", (fp,))
    prev_text = ""
    n_runs = 0
    history = [ None ] * len(CONSENSUSES)
    for (rs_text,published) in cur.fetchall():
        rs = stem.descriptor.router_status_entry.RouterStatusEntryMicroV3(bytes(rs_text))
        history[CONSENSUSES[published]] = rs
        if rs_text != prev_text:
            prev_text = rs_text
            n_runs += 1

    differences = []
    if True:
        differences = find_differences(history)

    print("{} => {} {} / {} {}".format(fp, n_mds, n_runs, n_consensuses, " ".join(differences)))

def list_relays(connection, limit=None):
    cur = connection.cursor()
    if limit is not None:
        cur.execute("select * from relays limit ?;", (limit,));
    else:
        cur.execute("select * from relays;")
    return set(r for (r,) in cur.fetchall())

def load_consensuses(connection):
    cur = connection.cursor()
    cur.execute("select * from consensuses;")
    names = sorted(c for (c,) in cur.fetchall())
    result = {}
    for name, n in zip(names, itertools.count()):
        result[name] = n
    return result

with sqlite3.connect(DBNAME) as connection:
    CONSENSUSES = load_consensuses(connection)
    relays = list_relays(connection)
    for r in sorted(relays):
        analyze_relay(r, connection)
