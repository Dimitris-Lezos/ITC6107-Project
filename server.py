import socket
import time
import random
from datetime import date, timedelta
import holidays

PORT = 9999

start_date = date(2022, 12, 1)

holidays_Greece = holidays.Greece()

stock = [
    ('IBM', 128.25), ('AAPL', 151.60), ('FB', 184.51), ('AMZN', 93.55),
    ('GOOG', 93.86), ('META', 184.51), ('MSI', 265.96), ('INTC', 25.53),
    ('AMD', 82.11), ('MSFT', 254.15), ('DELL', 38.00), ('ORKL', 88.36),
    ('HPQ', 27.66), ('CSCO', 48.91), ('ZM', 69.65), ('QCOM', 119.19),
    ('ADBE', 344.80), ('VZ', 37.91), ('TXN', 172.06), ('CRM', 182.32),
    ('AVGO', 625.15), ('NVDA', 232.88), ('VMW', 120.05), ('EBAY', 43.98)
]

# ssocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
## ssocket.bind((socket.gethostname(), PORT))
# ssocket.bind(('', PORT))
# ssocket.listen()
print("Server ready: listening to port {0} for connections.\n".format(PORT))
# (c, addr) = ssocket.accept()

c_date = start_date
while c_date < date.today():
    if c_date.weekday() < 5 and c_date not in holidays_Greece:
        for s in stock:
            ticker, price = s
            jobj = '{{"TICKER": "{0}", "PRICE": "{1:.2f}", "DATE": "{2}"}}'.format(ticker, price, c_date)
            print(jobj)
#            c.send((msg + '\n').encode())
        stock = list(map(lambda x: (x[0], round(x[1] * (1 + random.random() / 10 - 0.05), 2)), stock))
    c_date += timedelta(days=1)
    time.sleep(1)
