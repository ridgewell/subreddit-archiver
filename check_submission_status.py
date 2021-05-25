# -*- coding: utf-8 -*-

import praw
import psycopg2
import datetime
from psycopg2 import OperationalError

DB=""
DB_USER=""
DB_PASS=""
DB_HOST="127.0.0.1"
DB_PORT="5432"
target_subreddit=""

reddit = praw.Reddit(
    client_id="",
    client_secret="",
    user_agent="subreddit archiver bot",
    check_for_async=False
)

## Create connection with postgreSQL

def create_connection(db_name, db_user, db_password, db_host, db_port):
    connection = None
    try:
        connection = psycopg2.connect(
            database=db_name,
            user=db_user,
            password=db_password,
            host=db_host,
            port=db_port,
        )
        print("Connection to PostgreSQL DB successful")
    except OperationalError as e:
        print(f"The error '{e}' occurred")
    return connection

connection = create_connection(
    DB, DB_USER, DB_PASS, DB_HOST, DB_PORT
)

extract_past_24h = '''SELECT id,selftext,is_self FROM PUBLIC.submissions WHERE datetime >= NOW() - '24 hours'::INTERVAL AND deleted = FALSE;'''

cursor = connection.cursor()
cursor.execute(extract_past_24h)
lot = cursor.fetchall()
print("Number of submissions to check: ", len(lot))


mark_deleted = '''UPDATE public.submissions SET "deleted" = True WHERE id = %s;'''
mark_edited = '''UPDATE public.submissions SET "edited" = True, edited_body = %s WHERE id = %s;'''
cursor = connection.cursor()

def main():
    for i in range(0, len(lot)):
        lot_length = len(lot)
        submission = reddit.submission(lot[i][0])
        if submission.author == None:
            print("Submission was deleted. (Submission ID: %s)" % lot[i][0])
            cursor.execute(mark_deleted, (lot[i][0],))
            connection.commit()
            print("Progress: %s / %s" % (i + 1, lot_length))

        elif (lot[i][2] and submission.selftext != lot[i][1]):
            print("Submission was edited. (Submission ID: %s)" % lot[i][0])
            cursor.execute(mark_edited, (reddit.submission(lot[i][0]).selftext,
                                         lot[i][0]))
            connection.commit()
            print("Progress: %s / %s" % (i + 1, lot_length))
        else:
            print("No changes were identified. (Submission ID: %s) " % lot[i][0])
            print("Progress: %s / %s" % (i + 1, lot_length))

main()