from ib_async import IB, Stock

HOST = '127.0.0.1'
PORT = 7497
CLIENT_ID = 98
SYMBOLS = ['SPY', 'DIA']

errors = []
ib = IB()

if hasattr(ib, 'errorEvent'):
    def on_error(req_id, error_code, error_string, contract=None):
        errors.append((req_id, error_code, error_string, getattr(contract, 'symbol', None)))
        print(
            f"ERROR_EVENT reqId={req_id} code={error_code} msg={error_string} "
            f"symbol={getattr(contract, 'symbol', None)}"
        )
    ib.errorEvent += on_error

print(f'Connecting to {HOST}:{PORT} clientId={CLIENT_ID}')
ib.connect(HOST, PORT, clientId=CLIENT_ID)
print(f'connected={ib.isConnected()}')
contracts = ib.qualifyContracts(*[Stock(sym, 'SMART', 'USD') for sym in SYMBOLS])
print('qualified=', [c.symbol for c in contracts])
subscriptions = []
for contract in contracts:
    try:
        ticker = ib.reqTickByTickData(contract, 'AllLast')
        subscriptions.append((contract, ticker))
        print(f'requested_tbt symbol={contract.symbol}')
        ib.sleep(2)
    except Exception as exc:
        print(f'EXCEPTION symbol={contract.symbol} err={exc}')

ib.sleep(3)
for contract, ticker in subscriptions:
    ticks = getattr(ticker, 'ticks', None)
    count = 0 if ticks is None else len(ticks)
    print(f'status symbol={contract.symbol} tick_count={count}')
    try:
        ib.cancelTickByTickData(contract, 'AllLast')
    except Exception as exc:
        print(f'cancel exception symbol={contract.symbol} err={exc}')

ib.disconnect()
print('final_errors=', errors)

