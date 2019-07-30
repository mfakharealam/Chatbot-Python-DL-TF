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


def find_existing_score(p_id):
    try:
        sql = "SELECT score FROM parent_reply WHERE parent_id = '{}' LIMIT 1".format(p_id)
        cursor.execute(sql)
        result = cursor.fetchone()
        if result != None:
            return result[0]
        else:
            return False
    except Exception as e:
        return False


def acceptable(data):   # even consider a comment or not
    if len(data.split(' ')) > 50 or len(data) < 1:
        return False
    elif len(data) > 1000:
        return False
    elif data == '[deleted]' or data == '[removed]':
        return False
    else:
        return True


def format_data(data):
    data = data.replace("\n", " newlinechar ").replace("\r", " newlinechar ").replace('"', "'")
    return data


def find_parent(p_id):
    try:
        sql = "SELECT comment FROM parent_reply WHERE comment_id = '{}' LIMIT 1".format(p_id)
        cursor.execute(sql)
        result = cursor.fetchone()
        if result != None:
            return result[0]
        else:
            return False
    except Exception as e:
        return False


def transaction_builder(sql):   # to commit insertion statements in groups rather than one by one
    global sql_transaction
    sql_transaction.append(sql)
    if len(sql_transaction) > 1000:
        cursor.execute('BEGIN TRANSACTION')
        for sql_s in sql_transaction:
            try:
                cursor.execute(sql_s)
            except:
                pass
        connection.commit()
        sql_transaction = []    # empty it out


def sql_insert_replace_comment(comm_id, p_id, parent, comm, subred, time, scor):
    try:
        sql = """UPDATE prent_reply SET parent_id = ?, comment_id = ?, parent = ?, comment = ?, subreddit = ?,
        unix = ?, score = ? WHERE parent_id = ?;""".format(p_id, comm_id, parent, comm, subred, int(time), scor, p_id)
        transaction_builder(sql)
    except Exception as e:
        print("During insert&replace ", str(e))
        return False


def sql_insert_has_parent(c_id, p_id, p, c, sub, t, sc):
    try:
        sql = """INSERT INTO parent_reply (parent_id, comment_id, parent, comment, subreddit, unix, score)
        VALUES ("{}", "{}", "{}", "{}", "{}", {} ,{})""".format(p_id, c_id, p, c, sub, int(t), sc)
        transaction_builder(sql)
    except Exception as e:
        print("During insertion1 ", str(e))
        return False


def sql_insert_no_parent(c_id, p_id, c, sub, t, sc):
    try:
        sql = """INSERT INTO parent_reply (parent_id, comment_id, comment, subreddit, unix, score)
        VALUES ("{}", "{}", "{}", "{}", "{}", {} ,{})""".format(p_id, c_id, c, sub, int(t), sc)
        transaction_builder(sql)
    except Exception as e:
        print("During insertion2 ", str(e))
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
            comment_id = row['comment_id']
            body = format_data(row['body'])     # clean up data
            created_utc = row['created_utc']
            score = row['score']
            subreddit = row['subreddit']

            parent_data = find_parent(parent_id)

            if score >= 2:      # comment score is good enough
                if acceptable(body):
                    existing_comment_score = find_existing_score(parent_id)
                    if existing_comment_score:
                        if score > existing_comment_score:
                            sql_insert_replace_comment(comment_id, parent_id, parent_data, subreddit, created_utc, score)
                    else:
                        if parent_data:  # it means, it's a reply
                            sql_insert_has_parent(comment_id, parent_id, parent_data, subreddit, created_utc, score)
                            paired_rows += 1
                        else:
                            sql_insert_no_parent(comment_id, parent_id, body, subreddit, created_utc, score)

            if row_counter % 100000 == 0:
                print("Total rows read: {}, Paired rows: {}, Time: {}").format(row_counter, paired_rows, str(datetime.now()))
