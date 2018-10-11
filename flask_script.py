import threading
import time
# class Manage():
#     def command(self, xyz):
#         xyz()
#         print("command")

def show_count(start, end):
    t = time.time()
    for i in list(range(start,end)):
        print(i)
        time.sleep(i / 1000)
    print(time.time() - t)
show_count(0,50)


# thread = threading.Thread(target=show_count, args=(0, 10))
# thread1 = threading.Thread(target=show_count, args=(10, 20))
# thread2 = threading.Thread(target=show_count, args=(20, 30))
# thread3 = threading.Thread(target=show_count, args=(30, 40))
# thread4 = threading.Thread(target=show_count, args=(40, 50))
#
# try:
#     t = time.time()
#     thread.start()
#     thread1.start()
#     thread2.start()
#     thread3.start()
#     thread4.start()
#     thread.join()
#     thread1.join()
#     thread2.join()
#     thread3.join()
#     thread4.join()
#     print(time.time() - t)
# except:
#     print("error")

