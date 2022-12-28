from Firebase import firebaseDB
import time
from multiprocessing import Pool
import requests
import os

def getDocument(collection, docId):
  print 'getDocuments params ~>', collection, docId
  data = firebaseDB.collection(collection).document(docId).get().to_dict()
  # As python.pickle cannot serialize and deserialize firebase data types, we resort to fixing them by manipulating them into dictionary
  pickleFields = []
  if collection == 'trades':
    pickleFields = ['order']
  elif collection == 'orders':
    pickleFields = ['account', 'pair']
  elif collection == 'accountNormalized':
    for index, token in enumerate(data['tokens']):
      data['tokens'][index]['currency'] = {'id': token['currency'].id}
  for field in pickleFields:
    if field in data:
      data[field] = { 'id': data[field].id }
  return data

def updateDocument(collection, docId, params):
  firebaseDB.collection(collection).document(docId).update(params)
  return True

def resetTokenCurrency(token):
  token['currency'] = firebaseDB.collection('currencies').document(token['currency']['id'])
  return token

def _process_trades(trades, pairName):
  print 'prcess_trade commenced ~>', time.asctime( time.localtime(time.time()) )
  print 'with argument', trades, pairName
  if trades is None:
    return
  for trade in trades:
    party1 = trade['party1']
    party2 = trade['party2']
    trade1Id = party1[0]
    trade2Id = party2[0]

    # Construct the trade documents
    print '44'
    pool = Pool()
    print 'trade documents retrieve commenced', time.asctime( time.localtime(time.time()) ) 
    promises = [
      pool.apply_async(getDocument, ['trades', trade1Id]),
      pool.apply_async(getDocument, ['trades', trade2Id])
    ]
    trade1, trade2 = map(lambda p : p.get(), promises)
    print 'trade documents retrieve ended', time.asctime( time.localtime(time.time()) )
    order1Id = trade1['order']['id']
    order2Id = trade2['order']['id']

    # Construct the order documents
    print 'order documents retrieve commenced', time.asctime( time.localtime(time.time()) )
    promises = [
      pool.apply_async(getDocument, ['orders', order1Id]), 
      pool.apply_async(getDocument, ['orders', order2Id])
    ]    
    order1, order2 = map(lambda p : p.get(), promises)
    print 'order documents retrieve ended', time.asctime( time.localtime(time.time()) )

    # Construct the user account documents
    print 'account documents retrieve commenced', time.asctime( time.localtime(time.time()) )
    promises = [
      pool.apply_async(getDocument, ['accountNormalized', order1['account']['id']]),
      pool.apply_async(getDocument, ['accountNormalized', order2['account']['id']])
    ]
    user1, user2 = map(lambda p : p.get(), promises)
    print 'account documents retrieve ended', time.asctime( time.localtime(time.time()) )
    user1Tokens = user1['tokens']
    user2Tokens = user2['tokens']

    # initialize update_promises
    update_promises = []

    # Calculate quantity and total
    quantity = int(trade['quantity'])
    total = float(trade['price']) * quantity

    # Update order records
    order1FilledAmount = order1['filledAmount'] if 'filledAmount' in order1 else 0
    order2FilledAmount = order2['filledAmount'] if 'filledAmount' in order2 else 0
    update_promises.append(pool.apply_async(updateDocument, ['orders', order1Id, {
      'status': (u"closed" if (quantity + order1FilledAmount) == order1['amount'] else u"partially_filled"),
      'filledAmount': order1FilledAmount + quantity
    }]))
    update_promises.append(pool.apply_async(updateDocument, ['orders', order2Id, {
      'status': (u"closed" if (quantity + order2FilledAmount) == order2['amount'] else u"partially_filled"),
      'filledAmount': order2FilledAmount + quantity
    }]))
    
    # Update trade records
    if (quantity + order1FilledAmount) == order1['amount']:
      update_promises.append(pool.apply_async(updateDocument, ['trades', trade1Id, {
        'status': u"closed"
      }]))
    if (quantity + order2FilledAmount) == order2['amount']:
      update_promises.append(pool.apply_async(updateDocument, ['trades', trade2Id, {
        'status': u"closed"
      }]))
    # initialize user1 and user2 concatenated promise payload
    user1PromisePayload = { 'accountId': order1['account']['id'], 'address': user1['publicKey'], 'type': 'promise' }
    user2PromisePayload = { 'accountId': order2['account']['id'], 'address': user2['publicKey'], 'type': 'promise' }
    # Update account token balances
    fromPair, toPair = pairName.split('/')
    for i, token in enumerate(user1Tokens):
      thisTokenName = token['name']
      if thisTokenName == fromPair:
        if party1[1] == 'bid':
          user1Tokens[i]['balances']['trading'] -= total
        else:
          user1Tokens[i]['balances']['promise'] += total
          user1PromisePayload['currencyId'] = user1Tokens[i]['currency']['id']
          user1PromisePayload['amount'] = total
      elif thisTokenName == toPair:
        if party1[1] == 'bid':
          user1Tokens[i]['balances']['promise'] += quantity
          user1PromisePayload['currencyId'] = user1Tokens[i]['currency']['id']
          user1PromisePayload['amount'] = quantity
        else:
          user1Tokens[i]['balances']['trading'] -= quantity
      else:
        continue
    for i, token in enumerate(user2Tokens):
      thisTokenName = token['name']
      if thisTokenName == fromPair:
        if party2[1] == 'bid':
          user2Tokens[i]['balances']['trading'] -= total
        else:
          user2Tokens[i]['balances']['promise'] += total
          user2PromisePayload['currencyId'] = user2Tokens[i]['currency']['id']
          user2PromisePayload['amount'] = total
      elif thisTokenName == toPair:
        if party2[1] == 'bid':
          user2Tokens[i]['balances']['promise'] += quantity
          user2PromisePayload['currencyId'] = user2Tokens[i]['currency']['id']
          user2PromisePayload['amount'] = quantity
        else:
          user2Tokens[i]['balances']['trading'] -= quantity
      else:
        continue
    user1Tokens = map(resetTokenCurrency, user1Tokens)
    user2Tokens = map(resetTokenCurrency, user2Tokens)
    print 'updating user accounts commented', time.asctime( time.localtime(time.time()) )
    firebaseDB.collection('accountNormalized').document(order1['account']['id']).update({
      'tokens': user1Tokens
    })
    firebaseDB.collection('accountNormalized').document(order2['account']['id']).update({
      'tokens': user2Tokens
    })
    print 'updating user accounts ended', time.asctime( time.localtime(time.time()) )

    print 'Generate concatenated promise commenced', time.asctime( time.localtime(time.time()) )
    # Generate concatenated promise
    LOOPBACK_URL = os.environ['LOOPBACK_URL'] if 'LOOPBACK_URL' in os.environ else 'http://localhost:3001/api/orders/generateProofOrPromise'
    requests.post(LOOPBACK_URL, data=user1PromisePayload)
    requests.post(LOOPBACK_URL, data=user2PromisePayload)
    print 'Generate concatenated promise ended', time.asctime( time.localtime(time.time()) )

    print 'update_promises commenced', time.asctime( time.localtime(time.time()) )
    for promise in update_promises:
      promise.get()
    print 'update_promises ended', time.asctime( time.localtime(time.time()) )
    
    print 'process_trade ended ~>', time.asctime( time.localtime(time.time()) )
    return True

def process_trades(trades, pairName):
  print 'prcess_trade commenced ~>', time.asctime( time.localtime(time.time()) )
  print 'with argument', trades, pairName
  if trades is None:
    return
  for trade in trades:
    party1 = trade['party1']
    party2 = trade['party2']
    trade1Id = party1[0]
    trade2Id = party2[0]

    # Construct the trade documents
    print '44'
    # pool = Pool()
    print 'trade documents retrieve commenced', time.asctime( time.localtime(time.time()) ) 

    trade1 = firebaseDB.collection('trades').document(trade1Id).get().to_dict()
    trade2 = firebaseDB.collection('trades').document(trade2Id).get().to_dict()
    print 'trade documents retrieve ended', time.asctime( time.localtime(time.time()) )
    order1Id = trade1['order'].id
    order2Id = trade2['order'].id

    # Construct the order documents
    print 'order documents retrieve commenced', time.asctime( time.localtime(time.time()) )
    order1 = firebaseDB.collection('orders').document(order1Id).get().to_dict()
    order2 = firebaseDB.collection('orders').document(order2Id).get().to_dict()
    print 'order documents retrieve ended', time.asctime( time.localtime(time.time()) )

    # Construct the user account documents
    print 'account documents retrieve commenced', time.asctime( time.localtime(time.time()) )
    # promises = [
    #   pool.apply_async(getDocument, ['accountNormalized', order1['account']['id']]),
    #   pool.apply_async(getDocument, ['accountNormalized', order2['account']['id']])
    # ]
    # user1, user2 = map(lambda p : p.get(), promises)
    user1 = firebaseDB.collection('accountNormalized').document(order1['account'].id).get().to_dict()
    user2 = firebaseDB.collection('accountNormalized').document(order2['account'].id).get().to_dict()
    print 'account documents retrieve ended', time.asctime( time.localtime(time.time()) )
    user1Tokens = user1['tokens']
    user2Tokens = user2['tokens']

    # Calculate quantity and total
    quantity = int(trade['quantity'])
    total = float(trade['price']) * quantity

    # Update order records
    order1FilledAmount = order1['filledAmount'] if 'filledAmount' in order1 else 0
    order2FilledAmount = order2['filledAmount'] if 'filledAmount' in order2 else 0

    firebaseDB.collection('orders').document(order1Id).update({
      'status': (u"closed" if (quantity + order1FilledAmount) == order1['amount'] else u"partially_filled"),
      'filledAmount': order1FilledAmount + quantity
    })
    firebaseDB.collection('orders').document(order2Id).update({
      'status': (u"closed" if (quantity + order2FilledAmount) == order2['amount'] else u"partially_filled"),
      'filledAmount': order2FilledAmount + quantity
    })
    
    # Update trade records
    if (quantity + order1FilledAmount) == order1['amount']:
      firebaseDB.collection('trades').document(trade1Id).update({
        'status': u"closed"
      })
    if (quantity + order2FilledAmount) == order2['amount']:
      firebaseDB.collection('trades').document(trade1Id).update({
        'status': u"closed"
      })
    # initialize user1 and user2 concatenated promise payload
    user1PromisePayload = { 'accountId': order1['account'].id, 'address': user1['publicKey'], 'type': 'promise' }
    user2PromisePayload = { 'accountId': order2['account'].id, 'address': user2['publicKey'], 'type': 'promise' }
    # Update account token balances
    fromPair, toPair = pairName.split('/')
    for i, token in enumerate(user1Tokens):
      thisTokenName = token['name']
      if thisTokenName == fromPair:
        if party1[1] == 'bid':
          user1Tokens[i]['balances']['trading'] -= total
        else:
          user1Tokens[i]['balances']['promise'] += total
          user1PromisePayload['currencyId'] = user1Tokens[i]['currency'].id
          user1PromisePayload['amount'] = total
      elif thisTokenName == toPair:
        if party1[1] == 'bid':
          user1Tokens[i]['balances']['promise'] += quantity
          user1PromisePayload['currencyId'] = user1Tokens[i]['currency'].id
          user1PromisePayload['amount'] = quantity
        else:
          user1Tokens[i]['balances']['trading'] -= quantity
      else:
        continue
    for i, token in enumerate(user2Tokens):
      thisTokenName = token['name']
      if thisTokenName == fromPair:
        if party2[1] == 'bid':
          user2Tokens[i]['balances']['trading'] -= total
        else:
          user2Tokens[i]['balances']['promise'] += total
          user2PromisePayload['currencyId'] = user2Tokens[i]['currency'].id
          user2PromisePayload['amount'] = total
      elif thisTokenName == toPair:
        if party2[1] == 'bid':
          user2Tokens[i]['balances']['promise'] += quantity
          user2PromisePayload['currencyId'] = user2Tokens[i]['currency'].id
          user2PromisePayload['amount'] = quantity
        else:
          user2Tokens[i]['balances']['trading'] -= quantity
      else:
        continue

    print 'updating user accounts commented', time.asctime( time.localtime(time.time()) )
    print 'user1Tokens and user2Tokens'
    print user1Tokens, user2Tokens
    firebaseDB.collection('accountNormalized').document(order1['account'].id).update({
      'tokens': user1Tokens
    })
    firebaseDB.collection('accountNormalized').document(order2['account'].id).update({
      'tokens': user2Tokens
    })
    print 'updating user accounts ended', time.asctime( time.localtime(time.time()) )

    print 'Generate concatenated promise commenced', time.asctime( time.localtime(time.time()) )
    # Generate concatenated promise
    LOOPBACK_URL = os.environ['LOOPBACK_URL'] if 'LOOPBACK_URL' in os.environ else 'http://localhost:3001/api/orders/generateProofOrPromise'
    requests.post(LOOPBACK_URL, data=user1PromisePayload)
    requests.post(LOOPBACK_URL, data=user2PromisePayload)
    print 'Generate concatenated promise ended', time.asctime( time.localtime(time.time()) )
    
    print 'process_trade ended ~>', time.asctime( time.localtime(time.time()) )
    return True