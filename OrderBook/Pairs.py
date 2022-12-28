import ast
from Customorderbook import OrderBook
from Redis import _redis
import pprint
from Firebase import firebaseDB

pp = pprint.PrettyPrinter(indent=2).pprint

SUPPORTED_PAIRS = {
  'ETH/BTC': { 'redisKey': 'ETH_BTC_ORDER_BOOK', 'orderBook': OrderBook() },
  'LTC/BTC': { 'redisKey': 'LTC_BTC_ORDER_BOOK', 'orderBook': OrderBook() },
}

# # [ { 'party1': [u'MquaS9V1NbV0uZ6I5aIa', 'ask', u'CHVJqWoCHntVYFbZgjVP'],
# #     'party2': [u'bwpCrNmbQm2UhBncL6i1', 'bid', None],
# #     'price': Decimal('4'),
# #     'quantity': 10,
# #     'time': 1534659945,
# #     'timestamp': 1534659945}]
for pair in SUPPORTED_PAIRS:
  redisKey = SUPPORTED_PAIRS[pair]['redisKey']
  targetOrderBook = SUPPORTED_PAIRS[pair]['orderBook']
  pairDataFromRedis = _redis.lrange(redisKey, 0, -1)
  print '###BEGIN Processing ' + pair + ' FROM REDIS'
  for order in pairDataFromRedis:
    targetOrderBook.process_order(ast.literal_eval(order), True, True)
  print '###END Processing ' + pair + ' FROM REDIS'
