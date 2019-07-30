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


def format_data(data):
    data = data.replace("\n", " newlinechar ").replace("\r", " newlinechar ").replace('"', "'")
    return data


def find_parent(parent_id):
    try:
        sql = "SELECT comment FROM parent_reply WHERE comment_id = '{}' LIMIT 1".format(parent_id)
        cursor.execute(sql)
        result = cursor.fetchone()
        if result != None:
            return result[0]
        else:
            return False
    except Exception as e:
        return False


if __name__ == "__main__":
    create_table()
    row_counter = 0     # db rows
    paired_rows = 0     # parent & child pairs in comments(reddit)
    with open("C:/Users/Muhammad Fakhar/PycharmProjects/RC_2015-01", buffering=1000) as file:
        for row in file:
            row_counter += 1
            row = json.loads(row)
            parent_id = row['parent_id']
            body = format_data(row['body'])     # clean up data
            created_utc = row['created_utc']
            score = row['score']
            subreddit = row['subreddit']

            parent_data = find_parent(parent_id)
