import sys
from flask_script import Manage
# firstarg = int(sys.argv[1])
# print(firstarg + 1)
manage = Manage()
def helloWorld():
    print("Hello World")

# def test():
#     print("test")

@manage.command
def test():
    print("test")

