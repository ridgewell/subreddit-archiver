# -*- coding: utf-8 -*-
import praw
import datetime
import psycopg2
from psycopg2 import OperationalError
import time

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

def execute_query(connection, query):
    connection.autocommit = True
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        print("Query executed successfully")
    except OperationalError as e:
        print(f"The error '{e}' occurred")
        
create_comments_table = """
CREATE TABLE IF NOT EXISTS comments (
  subreddit TEXT,
  datetime TIMESTAMP,
  utc_time NUMERIC,
  id TEXT PRIMARY KEY,
  author TEXT NOT NULL, 
  body TEXT,
  distinguished TEXT,
  is_submitter BOOLEAN,
  parent_id TEXT,
  permalink TEXT,
  submission TEXT,
  submission_id TEXT,
  deleted BOOLEAN DEFAULT FALSE,
  edited BOOLEAN DEFAULT False,
  edited_body TEXT
)
"""

execute_query(connection, create_comments_table)

def pipe_comments(target):
    print("Comment piping initiated on subreddit(s): ", target)
    subreddit = reddit.subreddit(target)
    connection.autocommit = True
    cursor = connection.cursor()
    try:
        for comment in subreddit.stream.comments(skip_existing=True):
            postgres_insert_query = """ INSERT INTO comments (subreddit, datetime, utc_time, id, author, body, distinguished, is_submitter, parent_id, permalink, submission, submission_id) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
            record_to_insert = (comment.subreddit.display_name,
                                datetime.datetime.utcfromtimestamp(comment.created_utc),
                                comment.created_utc,
                                comment.id,
                                comment.author.name,
                                comment.body,
                                comment.distinguished,
                                comment.is_submitter,
                                comment.parent_id,
                                comment.permalink,
                                comment.submission.title,
                                comment.submission.id
                                )
            cursor.execute(postgres_insert_query, record_to_insert)
            print("Comment inserted. (ID: %s, Author: %s, Subreddit: %s" % (comment.id, comment.author, comment.subreddit.display_name))
    except Exception as err:
        '''
        Try catch to account for reddit API errors/downtime.
        May result in some comments during this interval in not being extracted. Not yet tested.
        '''
        print("Error occurred:", err)
        print("Retrying in 5 minutes.")
        time.sleep(300)
        pipe_comments(target)
        
pipe_comments(target_subreddit)
        
