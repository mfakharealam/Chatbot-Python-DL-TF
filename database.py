import sqlite3
import json
from datetime import datetime

timeframe = '2015-01'
sql_transaction = []  # will do bunch of queries altogether
connection = sqlite3.connect('{}.db'.format(timeframe))
cursor = connection.cursor()


def create_table():
    cursor.execute("""CREATE TABLE IF NOT EXISTS Parent_Reply(parent_id TEXT PRIMARY KEY, comment_id TEXT UNIQUE, 
    parent TEXT, comment TEXT, subreddit TEXT, unix INT, score INT)""")     # three double quotes are for division


if __name__ == "__main__":
    create_table()
