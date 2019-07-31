import sqlite3
import pandas as pd

timeframes = ['2015-01']

for timeframe in timeframes:
    connection = sqlite3.connect()
    limit = 5000    # comments processing each time
    last_unix = 0   # helps in buffering data, after each iteration, comparing time
    curr_len = limit
    counter = 0
    test_done = False
    while curr_len == limit:
        dataframe = pd.read_sql("""SELECT * FROM parent_reply WHERE unix > {} AND parent NOT NULL
        AND score > 0 ORDER BY unix ASC LIMIT {}""".format(last_unix, limit), connection)
        last_unix = dataframe.tail(1)['unix'].values[0]
        curr_len = len(dataframe)
        if not test_done:
            with open("test.from", 'a', encoding='utf8') as file:
                for content in dataframe['parent'].values:
                    file.write(content + '\n')
            with open("test.to", 'a', encoding='utf8') as file:
                for content in dataframe['comment'].values:
                    file.write(content + '\n')
            test_done = True
        else:   # now do training
            with open("train.from", 'a', encoding='utf8') as file:
                for content in dataframe['parent'].values:
                    file.write(content + '\n')
            with open("train.to", 'a', encoding='utf8') as file:
                for content in dataframe['comment'].values:
                    file.write(content + '\n')

        counter += 1
        if counter % 20 == 0:
            print(counter * limit, ' rows completed so far')
