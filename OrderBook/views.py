# -*- coding: utf-8 -*-
from django.shortcuts import render
from django.http import Http404
from rest_framework.views import APIView
from rest_framework.decorators import api_view
from rest_framework.response import Response
from rest_framework import status
from django.http import JsonResponse
from django.conf import settings

from rest_framework import serializers
from rq import Queue

import pprint
import json
import time
import os
import papertrail

from Redis import _redis
from Pairs import SUPPORTED_PAIRS
from Firebase import firebaseDB
from ProcessTrades import process_trades, _process_trades

q = Queue(connection=_redis)
pp = pprint.PrettyPrinter(indent=2).pprint

class NewOrderSerializer(serializers.Serializer):
  _type = serializers.CharField()
  side = serializers.CharField()
  quantity = serializers.IntegerField()
  price = serializers.FloatField()
  # trade_id ~> the public key of the user
  trade_id = serializers.CharField()
  order_id = serializers.CharField()
  pair = serializers.CharField()

@api_view(["POST"])
def placeOrder(request):
  requestData = request.data
  serializedData = NewOrderSerializer(data=requestData)
  isValid = serializedData.is_valid()
  print 'isValid ~>', isValid
  if (isValid is not True):
    return JsonResponse({'success': False, 'message': 'Invalid data'}, safe=False)
  if requestData['pair'] in SUPPORTED_PAIRS:
    TARGET_KEY = SUPPORTED_PAIRS[requestData['pair']]['redisKey']
    TARGET_ORDER_BOOK = SUPPORTED_PAIRS[requestData['pair']]['orderBook']
  else:
    return JsonResponse({'message': 'pair ' + requestData['pair'] + ' is not supported'}, safe=False)
  newOrder = serializedData.data
  newOrder['type'] = newOrder['_type']
  newOrder.pop('_type')
  newOrder['timestamp'] = int(time.time())
  _redis.lpush(TARGET_KEY, newOrder)
  print 'newOrder ~>'
  pp(newOrder)
  trades, order_id = TARGET_ORDER_BOOK.process_order(newOrder, True, True)

  print 'trades ~>', trades
  # print 'order_id ~>', order_id
  # print 'order_book ~>', TARGET_ORDER_BOOK
  if trades != None and len(trades) > 0:
    # IS this a good idea?
    # Assuming process_trades will reach end of execution and then the thread will be killed
    # Assuming that the further subsequent orders won't kill or terminate the preceding active thread
    # q.enqueue(_process_trades, trades, requestData['pair'])
    process_trades(trades, requestData['pair'])
  return JsonResponse({'success': True}, safe=False)

@api_view(["GET"])
def pingTest(request):
  REDIS_HOST = os.environ['REDIS_HOST'] if 'REDIS_HOST' in os.environ else 'localhost'
  return JsonResponse({ 'message': 'ok', 'REDIS_HOST': REDIS_HOST  }, safe=False)