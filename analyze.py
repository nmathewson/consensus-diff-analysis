#!/usr/bin/python3

import sqlite3
import os
import stem.descriptor
DBNAME = "mdhistory.sqlite"
SRCDIR = "./microdescs-2017-01"

def md_filename(digest):
    return os.path.join(SRCDIR, "micro", digest[0], digest[1], digest)

def find_changes(md1, md2):
    differences = []
    for letter, attr in [('o', 'onion_key'),
                         ('O', 'ntor_onion_key'),
                         ('a', 'or_addressses'),
                         ('f', 'family'),
                         ('4', 'exit_policy'),
                         ('6', 'exit_policy_v6'),
                         ('k', 'identifiers'),
                         ('p', 'protocols')]:
        if getattr(md1, attr, None) != getattr(md2, attr, None):
            differences.append(letter)
    return "".join(differences)

def find_differences(mdlist):
    result = []
    prev_md = None
    for digest in mdlist:
        try:
            md = list(stem.descriptor.parse_file(md_filename(digest)))[0]
        except:
            print("whoops")
            continue

        if prev_md == None:
            prev_md = md
            continue

        result.append(find_changes(prev_md, md))
        prev_md = md
    return result

def analyze_relay(fp, connection):
    cur = connection.cursor()
    cur.execute("""select count(*) from history where relay_id = ?;""", (fp,))
    row = cur.fetchone()
    n_consensuses = row[0]

    cur.execute("""select md_id, count(*) from history where relay_id = ?
                   group by md_id order by con_published;""", (fp,))
    rows = cur.fetchall()
    n_mds = len(rows)

    cur.execute("""select md_id from history where relay_id = ?
                   order by con_published;""", (fp,))
    prev = ""
    n_runs = 0
    distinct_ordered = []
    for (mdid,) in cur.fetchall():
        if mdid != prev:
            prev = mdid
            n_runs += 1
            distinct_ordered.append(mdid)

    differences = []
    if True:
        differences = find_differences(distinct_ordered)

    print("{} => {} {} / {} {}".format(fp, n_mds, n_runs, n_consensuses, " ".join(differences)))

def list_relays(connection, limit=None):
    cur = connection.cursor()
    if limit is not None:
        cur.execute("select * from relays limit ?;", (limit,));
    else:
        cur.execute("select * from relays;")
    return set(r for (r,) in cur.fetchall())

with sqlite3.connect(DBNAME) as connection:
    relays = list_relays(connection)
    for r in sorted(relays):
        analyze_relay(r, connection)
