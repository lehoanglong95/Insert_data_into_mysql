# import pandas as pd
import glob
import os
import numpy as np
import pymysql
import threading
import time
connect = pymysql.connect(host='localhost', port=3306, user='root', password='chigiang85', db='Fb')
cursor = connect.cursor()
#
query = "REPLACE INTO phone_to_fb_part0(msisdn, fb_id, gender, dob, name, province, district, ward) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
# path = "/home/longle/Downloads/phone_profile_db"
# all_files = glob.glob(os.path.join(path, "*.csv"))
# np_array_list = []
# for file_ in all_files:
#     df = pd.read_csv(file_,index_col=None, header=0)
#     np_array_list.append(df.as_matrix())
#
# comb_np_array = np.vstack(np_array_list)
# big_df = pd.DataFrame(comb_np_array)
# def import_data(partition):
#     input = '/home/longle/Downloads/phone_profile_db/part-r-' + partition + '-cc305a94-cd77-4dde-b036-f7544b57f095.csv'
#     print(input)
# df = pd.read_csv('/home/longle/Downloads/phone_profile_db/*', header=None)
# big_df.drop_duplicates().to_csv("/home/longle/phone_profile_db.csv")
# for row in df.head(10):
#     print(row[0])
#     print(row[1])
# df.drop_duplicates(0)
# def import_data_into_sql(begin, end):
#     part = df[begin, end]
#     bulk = []
#
#     try:
#         cursor.execute(query, (str(part['msisdn'])), str(part['fb_id']))
#         connect.commit()
#     except:
#         pass
    # for index, row in part.iterrows():
    #     try:
    #         cursor.execute(query, (str(row['msisdn']), str(row['fb_id'])))
    #         connect.commit()
    #     except:
    #         pass

# # import_data_into_sql(1000)
#
# print(df.loc[132132])
# bulks = []
# len = df.count()
# df[0: 1000000]
# print(len)
#
# bulks = []
import pandas as pd

# df = pd.read_csv('/home/longle/Desktop/phone_to_fb/new_file',header=None)
df = pd.read_csv('/home/longle/Desktop/phone_to_fb/part-r-00000-9e544572-7f08-4303-9815-e99b28106f33.csv', header=None, low_memory=False)
# df.count()
len_value = len(df)
print(len_value)
def import_database(start, end):
    bulks = []
    connect = pymysql.connect(host='localhost', port=3306, user='root', password='chigiang85', db='Fb')
    cursor = connect.cursor()
    # print(type(bulks))
    for index, row in df[start: end].iterrows():
        bulks.append((str(row[0]), str(row[1]), str(row[2]), str(row[3]), str(row[4])[0:300], str(row[5]), str(row[6]), str(row[7])))
        if len(bulks) == 10000:
            try:
                cursor.executemany(query, bulks)
                connect.commit()
                bulks.clear()
            except:
                connect.rollback()
        elif index == end - 1:
            try:
                cursor.executemany(query, bulks)
                connect.commit()
                bulks.clear()
            except:
                connect.rollback()
# import_database(0, 100)
# import_database(0,14)
# import_database(0, 100)
#
# import_database(0, 1000000)

#
# import_database(0, 100)
# import_database(6000000, len_value)
try:
    t = time.time()
    t1 = threading.Thread(target=import_database, args=(0, 1000000))
    t2 = threading.Thread(target=import_database, args=(1000000, 2000000))
    t3 = threading.Thread(target=import_database, args=(2000000, 3000000))
    t4 = threading.Thread(target=import_database, args=(3000000, 4000000))
    t5 = threading.Thread(target=import_database, args=(4000000, 5000000))
    t6 = threading.Thread(target=import_database, args=(5000000, len_value))
    # t7 = threading.Thread(target=import_database, args=(6000000, 7000000))
    # t8 = threading.Thread(target=import_database, args=(7000000, 8000000))
    # t9 = threading.Thread(target=import_database, args=(8000000, 9000000))
    # t10 = threading.Thread(target=import_database, args=(9000000, 10000000))
    # t11 = threading.Thread(target=import_database, args=(10000000, len_value))
    t1.start()
    t2.start()
    t3.start()
    t4.start()
    t5.start()
    t6.start()
    # t7.start()
    # t8.start()
    # t9.start()
    # t10.start()
    # t11.start()
    t1.join()
    t2.join()
    t3.join()
    t4.join()
    t5.join()
    t6.join()
#     # t7.join()
#     # t8.join()
#     # t9.join()
#     # t10.join()
#     # t11.join()
    print(time.time() - t)
except:
    print("error")
# for index, row in df.drop_duplicates(1).iterrows():
#     print(str(row[1]))
#     print(str(row[0]))
#     print(int(row['fb_id']))
#     print(row[0], row[1])
    # print(type(row['fb_id']))
    # print(type(row['msisdn']))
    # bulks.append((str(row[1]), str(row[0])))
    # if len(bulks) == 10000:
    #     try:
            # cursor.executemany(query, bulks)
            # connect.commit()
            # bulks.clear()
        # except:
        #     pass
# array_index = [x for x in range(70835, 6000000)]
# # #

# with multiprocessing.Pool(10) as p:
#     p.map(import_data_into_sql, array_index)
#
# #
# # p1 = multiprocessing.Process(target=import_data_into_sql, args=(55178, 1000000))
# # p2 = multiprocessing.Process(target=import_data_into_sql, args=(1000001, 2000000))
# # p3 = multiprocessing.Process(target=import_data_into_sql, args=(2000001, 3000000))
# # p4 = multiprocessing.Process(target=import_data_into_sql, args=(3000001, 4000000))
# # p5 = multiprocessing.Process(target=import_data_into_sql, args=(4000001, 5000000))
# # p6 = multiprocessing.Process(target=import_data_into_sql, args=(5000001, 6000000))
# #
# # p1.start()
# # p2.start()
# # p3.start()
# # p4.start()
# # p5.start()
# # p6.start()
# # import_data_into_sql(55178, 1000000)