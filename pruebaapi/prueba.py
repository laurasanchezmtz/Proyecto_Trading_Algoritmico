from ib_async import *

ib = IB()

ib.connect('127.0.0.1', 7497, clientId=0)

print("Connected to IB")
print("ib.accountValues()")

ib.disconnect()

""""
Output:
open orders request timed out
Connected to IB
ib.accountValues()


"""