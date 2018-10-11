# def java_string_hashcode(s):
#     h = 0
#     for c in s:
#         h = (31 * h + ord(c)) & 0xFFFFFFFF
#     return ((h + 0x80000000) & 0xFFFFFFFF) - 0x80000000
#
#
# def partition(s):
#     raw_code = java_string_hashcode(s) % 20
#     if raw_code < 0:
#         raw_code = raw_code + 20
#     return raw_code
#
# print(partition('841647564128'))
import re

word = "841642611995asdasdas"
if re.match('^[0-9]*$', word):
    print("truee")
else:
    print("false")





# def function01(arg,name):
#     for i in range(arg):
#         print(name,'i---->',i,'\n')
#         print (name,"arg---->",arg,'\n')
#         sleep(1)
#
#
# def test01():
#     thread1 = Thread(target = function01, args = (10,'thread1', ))
#     thread1.start()
#     thread2 = Thread(target = function01, args = (10,'thread2', ))
#     thread2.start()
#     thread1.join()
#     thread2.join()
#     print ("thread finished...exiting")
#
#
#
# test01()