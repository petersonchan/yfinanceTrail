from yahooquery import Ticker

ticker = Ticker('AAPL')
data = ticker.summary_detail
print(data['AAPL']['beta'])