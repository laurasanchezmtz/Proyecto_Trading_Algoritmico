
from ib_async import IB

ib = IB()

try:
    ib.connect("127.0.0.1", 7497, clientId=11, timeout=10)

    print("Conectado a IB:", ib.isConnected())

    cuentas = ib.managedAccounts()
    print("Cantidad de cuentas:", len(cuentas))
    print("Cuenta paper identificada:",
          any(cuenta.startswith("DU") for cuenta in cuentas))

    valores = ib.accountValues()
    print("Valores de cuenta recibidos:", len(valores))

finally:
    ib.disconnect()
    print("Conexión cerrada")


""""
Output:
open orders request timed out
Connected to IB
ib.accountValues()


"""