#!/usr/bin/python

import sys
from datetime import datetime
import math
import sqlite3

def is_integer(s):
    try:
        int(s)
        return True
    except ValueError:
        return False

def is_number(s):
    try:
        float(s)
        return True
    except ValueError:
        return False

def is_date(s):
    try:
        datetime.strptime(s,'%m%d%Y')
        return True
    except ValueError:
        return False

def check_name(s):
    '''Check if name is empty or ill-formed'''
    if not s:
        return False
    if ', ' in s:
        names = s.split(', ',1)
        surname = names[0]
        if surname.isalpha()==False:
            return False
        others = names[1]
        if ' ' in others:
            first_name, middle_name = others.split(' ',1)
            if (first_name.isalpha()==False) or (middle_name.isalpha()==False):
                return False
        else:
            if others.isalpha()==False:
                return False
    else:
        if s.isalpha()==False:
            return False

    return True

def parse_line(line):

    split_line = line.split('|')
    # Total fields per line should be 21,ignore otherwise
    if len(split_line)!=21:
        return []
    # Ignore the record if has Other_ID
    if split_line[15]:
        return []
    # Ignore if CMTE_ID is empty
    if len(split_line[0])==0:
        return []

    CMTE_ID = split_line[0]
    # Ignore if donor name is empty or ill-formed
    if check_name(split_line[7].strip())==False:
        return []

    name = split_line[7].strip()
    # Ignore if zip code is less than 5 digits or not a integer
    if len(split_line[10])<5:
        return []

    if not is_integer(split_line[10][:5]):
        return []

    zip_code = split_line[10][:5]
    # Ignore if transaction_dt is not date
    if not is_date(split_line[13]):
        return []

    datetime_object = datetime.strptime(split_line[13],'%m%d%Y')
    year = datetime_object.year
    # Ignore if transaction_amouts is empty or not a number
    if len(split_line[14])==0:
        return []

    if not is_number(split_line[14]):
        return []

    amount = float(split_line[14])

    return [CMTE_ID,name,zip_code,year,amount]

def create_table():
    conn = sqlite3.connect(':memory:')

    conn.execute("""
    CREATE TABLE IF NOT EXISTS logs (
      id INTEGER PRIMARY KEY,
      cmte_id TEXT,
      zip_code TEXT,
      year INTEGER,
      amount REAL
      )
    """)
    return conn

def insert_record(conn,cur,entry):

    cur.execute('INSERT INTO logs(cmte_id,zip_code,year,amount) VALUES (?,?,?,?)',entry)

    conn.commit()

def summarize(cur,p,cmte_id,zip_code,year):

    cur.execute('''SELECT {amt} FROM {tn} WHERE {idf}=cmte_id AND {zc}=zip_code AND {yr}=year'''.\
                format(amt='amount',tn='logs',idf='cmte_id',zc='zip_code',yr='year'))

    donations = cur.fetchall()

    donations = [donation[0] for donation in donations]

    running_total = sum(donations)

    running_counts = len(donations)

    rank = int(math.ceil(p*1.0/100*len(donations)))
    running_percentile = donations[rank-1]

    return int(round(running_percentile)),int(round(running_total)),int(running_counts)

def write2file(path, data):
    with open(path,'a') as f:
        f.write(data+'\n')

if __name__ == "__main__":

    conn = create_table()
    cur = conn.cursor()

    log_file = './input/itcont.txt'
    stats_file = './input/percentile.txt'
    out_file = './output/repeat_donors.txt'

    contribution = open(log_file,'r')
    stats = open(stats_file,'r')
    # clear repeat_donors.txt
    out = open(out_file,'w+')
    out.close()

    percentile = int(stats.readline())
    stats.close()

    donor_dict = {}

    while True:

        line = contribution.readline()

        if not line:

            break

        line = line.strip()

        parsed = parse_line(line)

        if not parsed:

            continue

        donor_id = parsed[1]+' '+parsed[2]

        if donor_id in donor_dict:

            if parsed[3]>donor_dict[donor_id]:
                # Insert repeat donor entry into database
                entry = [parsed[0],parsed[2],parsed[3],parsed[4]]

                insert_record(conn,cur,entry)
                # Calculate running percentile, sum and counts
                query = [parsed[0],parsed[2],parsed[3]]

                p,total,count = summarize(cur,percentile,*query)

                outry = [parsed[0],parsed[2],str(parsed[3]),str(p),str(total),str(count)]
                # Write to output file
                write2file(out_file,'|'.join(outry))

            elif parsed[3]<donor_dict[donor_id]:
                # update donor's contribution year to an earlier year
                donor_dict[donor_id]=parsed[3]

        else:
            # set donor's contribution year
            donor_dict[donor_id]=parsed[3]

    contribution.close()
    conn.close()
