import pandas as pd
import pymysql
import threading
import time
import re
import sys

argv = sys.argv
table_name = argv(1)
input_name = argv(2)

connect = pymysql.connect(host='localhost', port=3306, user='root', password='chigiang85', db='Fb')
cursor = connect.cursor()


take_province_query = "SELECT * FROM Fb.districts"

province_lists = {}
district_lists = {}

cursor.execute(take_province_query)
results = cursor.fetchall()
# print(results)
for result in results:
    province_lists[str(result[3])] = int(result[2])
    district_lists[str(result[1])] = int(result[0])

print(province_lists)
print(district_lists)

# phone_to_fb_part0_test

query = "REPLACE INTO {}(msisdn, fb_id, gender, age, name, province, district, ward) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)".format(table_name)

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
# import pandas as pd

# df = pd.read_csv('/home/longle/Desktop/phone_to_fb/new_file',header=None)
# /home/longle/Desktop/phone_to_fb/part-r-00000-9e544572-7f08-4303-9815-e99b28106f33.csv
df = pd.read_csv('{}'.format(input_name), header=None, low_memory=False)
# df.count()
len_value = len(df)

def import_database(start, end):
    bulks = []
    connect = pymysql.connect(host='localhost', port=3306, user='root', password='chigiang85', db='Fb')
    cursor = connect.cursor()
    # print(type(bulks))
    for index, row in df[start: end].iterrows():
        if re.match('^[0-9]*$', str(row[0])):
            district_name = str(row[6]).replace("Thành Phố", "TP").replace("Thị Xã", "TX")
            province_name = str(row[5])
            province_id = -1
            district_id = -1
            gender = -1
            age = -1
            if province_lists.__contains__(province_name):
                province_id = province_lists[province_name]
            if district_lists.__contains__(district_name):
                district_id = district_lists[district_name]
            if str(row[2]) == 'M':
                gender = 1
            elif str(row[2]) == 'F':
                gender = 0
            if len(str(row[3])) >= 4:
                age = 2018 - int(str(row[3])[:4])
            # province_id = province_lists[str(row[5])]
            # district_id = district_lists[str(row[6])]
            bulks.append((str(row[0]), str(row[1]), gender, age, str(row[4])[0:300], province_id, district_id, str(row[7])))
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
