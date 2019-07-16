#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import hashlib
import sys
import sqlite3

def ResultIter(cursor, arraysize=1000):
    'An iterator that uses fetchmany to keep memory usage down'
    while True:
        results = cursor.fetchmany(arraysize)
        if not results:
            break
        for result in results:
            yield result

def init_db(filename):
    db = sqlite3.connect(filename)
    cursor = db.cursor()
    cursor.execute('''create table file (path text, hash text, size number)''')
    cursor.close()
    db.commit()
    return db

def get_short_digest(file_path):
    h = hashlib.md5()
    try:
        with open(file_path, 'rb') as file:
            #Just check the first 1024 bytes
            chunk = file.read(1024)
            if chunk:
                h.update(chunk)
    except FileNotFoundError:
        return 'not_found'
    return 's-' + h.hexdigest()
    
def get_digest(file_path):
    h = hashlib.sha256()
    try:
        with open(file_path, 'rb') as file:
            while True:
                # Reading is buffered, so we can read smaller chunks.
                chunk = file.read(h.block_size)
                if not chunk:
                    break
                h.update(chunk)
    except FileNotFoundError:
        return 'not_found'

    return h.hexdigest()

def recursive_add_to_index(path, db, cursor, total_done=0):
    for entry in os.scandir(path):
        if entry.is_dir():
            total_done = recursive_add_to_index(os.path.join(path, entry.name), db, cursor, total_done)
        else:
            size = os.path.getsize(entry.path)
            cursor.execute('insert into file (path, size) values (?, ?)', (entry.path, size))
            total_done += 1
            if total_done % 50 == 0:
                print("\r Indexing files: {}".format(total_done), end='\r')
    return total_done

def build_dir_index(path, index_filename='index.db', reset=False):
    if os.path.exists(index_filename):
        if reset:
            os.remove(index_filename)
        else:
            return sqlite3.connect(index_filename)
    db = init_db(index_filename)
    cursor = db.cursor()
    total_done = recursive_add_to_index(path, db, cursor, 0)
    cursor.close()
    db.commit()
    #print("{} files indexed successfully         ".format(total_done))
    return db

def print_progress(iteration, total, prefix = '', suffix = '', decimals = 1, length = 100, fill = '█'):
    """
    Call in a loop to create terminal progress bar
    @params:
        iteration   - Required  : current iteration (Int)
        total       - Required  : total iterations (Int)
        prefix      - Optional  : prefix string (Str)
        suffix      - Optional  : suffix string (Str)
        decimals    - Optional  : positive number of decimals in percent complete (Int)
        length      - Optional  : character length of bar (Int)
        fill        - Optional  : bar fill character (Str)
    """
    percent = ("{0:." + str(decimals) + "f}").format(100 * (iteration / float(total)))
    filledLength = int(length * iteration // total)
    bar = fill * filledLength + '-' * (length - filledLength)
    print('\r%s |%s| %s%% %s' % (prefix, bar, percent, suffix), end = '\r')
    # Print New Line on Complete
    if iteration == total: 
        print()

def hash_matching_files(db):
    # Hash files with identical file sizes
    cursor = db.cursor()
    update_cursor = db.cursor()
    cursor.execute('''select count(*) from file''')
    total = cursor.fetchone()[0]
    print("{} files found                ".format(total))
    cursor.execute('''select path, size, hash from file order by size, path''')
    last_size = 0
    last_path = ''
    last_digest = ''
    done = 0
    for path, size, digest in ResultIter(cursor):
        if size == last_size:
            if not digest:
                digest = get_short_digest(path)
                update_cursor.execute('''update file set hash=? where path=?''', (digest, path))
                if not last_digest:
                    last_digest = get_short_digest(last_path)
                    update_cursor.execute('''update file set hash=? where path=?''', (last_digest, last_path))
        last_size = size
        last_path = path
        last_digest = digest
        done += 1
        if done % 50 == 0:
            print_progress(done, total, 'Comparing')
    cursor.close()
    update_cursor.close()
    db.commit()

def files_match(file1, file2):
    return get_digest(file1) == get_digest(file2)

def dedupe(db, output_file, report_only=False):
    cursor = db.cursor()
    cursor.execute("select count(*) from file where hash <> ''")
    total = cursor.fetchone()[0]
    done = 0
    dupes = 0
    last_digest = ''
    last_path = ''
    cursor.execute('''select path, hash from file where hash <> '' order by hash, path''')
    for path, digest in ResultIter(cursor):
        if digest == last_digest:
            if files_match(last_path, path):
                dupes += 1
                output_file.write(path + '\n')
                if not report_only:
                    os.remove(path)
        last_digest = digest
        last_path = path
        done += 1
        if done % 50 == 0:
            print_progress(done, total, 'De-duping ')
    print("De-duping done                                                     ")
    print("{} duplicates detected".format(dupes))
    cursor.close()

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("path")
    args = parser.parse_args()
    full_path = os.path.abspath(args.path)
    if not os.path.isdir(full_path):
        print("Path must be a directory")
        sys.exit(1)
    print("Scanning {}".format(full_path))
    db = build_dir_index(full_path, reset=True)
    hash_matching_files(db)
    with open('duplicates.txt', 'w') as output:
        dedupe(db, output, report_only=False)
