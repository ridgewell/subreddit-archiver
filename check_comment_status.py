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

extract_past_12h = '''SELECT id,body FROM PUBLIC.comments WHERE datetime >= NOW() - '12 hours'::INTERVAL AND deleted = FALSE;'''

cursor = connection.cursor()
cursor.execute(extract_past_12h)
lot = cursor.fetchall()
print("Number of comments to check: ", len(lot))


mark_deleted = '''UPDATE public.comments SET "deleted" = True WHERE id = %s;'''
mark_edited = '''UPDATE public.comments SET "edited" = True, edited_body = %s WHERE id = %s;'''
cursor = connection.cursor()

def main():
    for i in range(0, len(lot)):
        comment = reddit.comment(lot[i][0])
        lot_length = len(lot)
        if comment.author == None:
            print("The comment was deleted. (Comment ID: %s)" % lot[i][0])
            cursor.execute(mark_deleted, (lot[i][0],))
            print("Progress: %s / %s" % (i + 1, lot_length))
            connection.commit()
        elif comment.body != lot[i][1]:
            print("The comment was edited. (Comment ID: %s)" % lot[i][0])
            cursor.execute(mark_edited, (reddit.comment(lot[i][0]).body,
                                         lot[i][0]))
            print("Progress: %s / %s" % (i + 1, lot_length))
            connection.commit()
        else:
            print("No changes were identified. (Comment ID: %s) " % lot[i][0])
            print("Progress: %s / %s" % (i + 1, lot_length))

main()