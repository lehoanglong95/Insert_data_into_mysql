import pandas as pd
import pymysql
import threading
import time
import sys

argv = sys.argv
table_name = argv[1]
input_name = argv[2]

connect = pymysql.connect(host='localhost', port=3306, user='root', password='chigiang85', db='Fb')
cursor = connect.cursor()

query = "REPLACE INTO {}(fb_id, msisdn) VALUES (%s, %s)".format(table_name)
df = pd.read_csv('{}'.format(input_name), header=None, low_memory=False, names=['fb_id', 'msisdn'])
new_df = df.drop_duplicates(['fb_id'], keep='last')
len_value = len(df)

def import_database(start, end):
    bulks = []
    connect = pymysql.connect(host='localhost', port=3306, user='root', password='chigiang85', db='Fb')
    cursor = connect.cursor()
    # print(type(bulks))
    for index, row in new_df[start: end].iterrows():
            bulks.append((str(row[0]), str(row[1])))
            if len(bulks) == 10000:
                # try:
                    cursor.executemany(query, bulks)
                    connect.commit()
                    bulks.clear()
                # except:
                #     connect.rollback()
            elif index == end - 1:
                # try:
                    cursor.executemany(query, bulks)
                    connect.commit()
                    bulks.clear()
                # except:
                #     connect.rollback()
#
try:
    t = time.time()
    t1 = threading.Thread(target=import_database, args=(0, 1000000))
    t2 = threading.Thread(target=import_database, args=(1000000, 2000000))
    t3 = threading.Thread(target=import_database, args=(2000000, 3000000))
    t4 = threading.Thread(target=import_database, args=(3000000, 4000000))
    t5 = threading.Thread(target=import_database, args=(4000000, 5000000))
    t6 = threading.Thread(target=import_database, args=(5000000, len_value))
    t1.start()
    t2.start()
    t3.start()
    t4.start()
    t5.start()
    t6.start()
    t1.join()
    t2.join()
    t3.join()
    t4.join()
    t5.join()
    t6.join()
    print(time.time() - t)
except:
    print("error")
