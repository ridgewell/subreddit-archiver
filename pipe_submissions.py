# -*- coding: utf-8 -*-
import praw
import datetime
import psycopg2
import time
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

def execute_query(connection, query):
    connection.autocommit = True
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        print("Query executed successfully")
    except OperationalError as e:
        print(f"The error '{e}' occurred")

create_submissions_table = """
CREATE TABLE IF NOT EXISTS submissions (
  subreddit TEXT,
  datetime TIMESTAMP,
  utc_time NUMERIC,
  id TEXT PRIMARY KEY,
  title TEXT,
  author TEXT, 
  url TEXT,
  permalink TEXT,
  selftext TEXT,
  distinguished TEXT,
  is_self BOOLEAN,
  deleted BOOLEAN DEFAULT FALSE,
  edited BOOLEAN DEFAULT FALSE,
  edited_body TEXT
)
"""

execute_query(connection, create_submissions_table)


def pipe_submissions(target):
    subreddit = reddit.subreddit(target)
    connection.autocommit = True
    cursor = connection.cursor()
    print("Submission piping initiated on subreddit(s): ", target)
    try:
        for submission in subreddit.stream.submissions(skip_existing=True):
            postgres_insert_query = """INSERT INTO submissions (subreddit, datetime, utc_time, id, title, author, url, permalink, selftext, distinguished, is_self) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
            record_to_insert = (submission.subreddit.display_name,
                                datetime.datetime.utcfromtimestamp(submission.created_utc),
                                submission.created_utc,
                                submission.id, 
                                submission.title, 
                                submission.author.name,
                                submission.url, 
                                submission.permalink, 
                                submission.selftext,
                                submission.distinguished,
                                submission.is_self)
            cursor.execute(postgres_insert_query, record_to_insert)
            print("Submission inserted. (ID: %s, Author: %s, Subreddit: %s)" % (submission.id, submission.author, submission.subreddit.display_name))
    except Exception as err:
        print("Error occurred:", err)
        print("Retrying in 5 minutes.")
        time.sleep(300)
        pipe_submissions(target_subreddit)

pipe_submissions(target_subreddit)
