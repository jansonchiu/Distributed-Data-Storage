
from hashlib import md5
from flask import Flask, json, request, jsonify
import os, sys, requests, threading, time
import random

api = Flask(__name__)

store = {}

replica_store = []
vector_clock = {}
queue = []

shard_store = {}
this_shard_id = None

socket_addr = os.environ.get('SOCKET_ADDRESS')
default_view = os.environ.get('VIEW').split(',')

# docker network create --subnet=10.10.0.0/16 mynet
# docker build -t assignment4-img .
# docker run -p 8082:8085 --net=mynet --ip=10.10.0.2 --name='node1' -e SOCKET_ADDRESS='10.10.0.2:8085' -e VIEW='10.10.0.2:8085,10.10.0.3:8085,10.10.0.4:8085,10.10.0.5:8085,10.10.0.6:8085,10.10.0.7:8085' -e SHARD_COUNT='2' assignment4-img


# Initializes the local replica store based on the corresponding environment variable
# Then broadcasts to add new replica to replica store if not apart of default view
def initialize_view():
  global replica_store
  replica_store = default_view
  json_body = { 'socket-address': socket_addr }
  broadcast_request('PUT', '/key-value-store-view', json_body) # Weird bug - two PUT requests are sent?

# Initializes the local shard store by assigning replica to shard iteratively/linearly
# Also sets a localized shard ID if specified
def initialize_shard():
  global this_shard_id
  if 'SHARD_COUNT' in os.environ:
    shard_store.clear()
    for i in range(len(replica_store)):
      shard_id = i % (int)(os.environ.get('SHARD_COUNT'))
      this_replica = replica_store[i]

      if this_replica == socket_addr:
        this_shard_id = shard_id
      shard_store.setdefault(shard_id, []).append(this_replica)

# Polls all other replicas for availability and broadcasts deletion request if replica is not available
def poll_replicas():
  while True:
    for replica_addr in default_view:
      if replica_addr != socket_addr:
        forward_url = 'http://' + replica_addr + '/internal'
        try:
          response = requests.get(forward_url)
        # except (requests.exceptions.Timeout, requests.exceptions.ConnectionError, ConnectionRefusedError):
        except:
          delete_view(replica_addr)
          json_body = { 'socket-address': replica_addr }
          broadcast_request('DELETE', '/key-value-store-view', json_body) # Safety measure
    time.sleep(10)

# Polls all other replicas and their vector clocks for causal consistency
def poll_vector_clock():
  global vector_clock, store
  while True:
    for replica_addr in default_view:
      if replica_addr != socket_addr:
        forward_url = 'http://' + replica_addr + '/internal?clock'
        try:
          response = requests.get(forward_url, timeout=1)
          remote_vector_clock = response.json().get('clock')
          if replica_addr in remote_vector_clock and (replica_addr not in vector_clock or remote_vector_clock[replica_addr] > vector_clock[replica_addr]):
              store = response.json().get('store')
              vector_clock = remote_vector_clock
              check_queue()
        except:
          continue
    time.sleep(10)

# Broadcasts a message globally or within a shard
def broadcast_request(request_type, target_endpoint, json_body=None, to_shard_replicas=False):
  if to_shard_replicas:
    local_store = shard_store[this_shard_id]
  else:
    local_store = replica_store

  for replica_addr in local_store:
    if replica_addr != socket_addr:
      forward_url = 'http://' + replica_addr + target_endpoint
      try:
        if request_type == 'PUT':
          response = requests.put(forward_url, json=json_body)
        if request_type == 'DELETE':
          response = requests.delete(forward_url, json=json_body)
      except:
        pass

# Internal route to manipulate or retrieve local variables
@api.route('/internal', methods=['GET', 'PUT', 'DELETE'])
def internal():
  global store, vector_clock, shard_store, this_shard_id
  if 'clock' in request.args:
    return json.dumps({'message': 'Clock retrieved successfully', 'clock': vector_clock}), 200
  elif 'store' in request.args:
    return json.dumps({'message': 'Store retrieved successfully', 'store': store}), 200
  elif 'put_key' in request.args:
    store[request.json.get('key')] = request.json.get('value')
    return json.dumps({'message': 'Key added successfully'}), 200
  elif 'delete_key' in request.args:
    store.pop(request.json.get('key'), None)
    return json.dumps({'message': 'Key deleted successfully'}), 200
  elif 'put_store' in request.args:
    store = request.json.get('store')
    return json.dumps({'message': 'Store updated successfully'}), 200
  elif 'update_store' in request.args:
    for replica_addr in shard_store[this_shard_id]:
      if replica_addr != socket_addr:
        neighbor_node = replica_addr
        break
    store = requests.get('http://' + neighbor_node + '/internal?store').json().get('store')
    return json.dumps({'message': 'Store updated successfully'}), 200
  elif 'update_shard_store' in request.args:
    shard_store = request.json.get('shard-store')
    this_shard_id = request.json.get('shard-id')
    return json.dumps({'message': 'Shard store updated successfully'}), 200
  elif 'increment' in request.args:
    vector_clock = get_incremented_clock(vector_clock, request.json.get('forwarded-address'))
    return json.dumps({'message': 'Vector clock updated successfully'}), 200
  elif 'reshard' in request.args:
    os.environ['SHARD_COUNT'] = str(request.json.get('shard-count'))
    initialize_shard()
    return json.dumps({'message': 'Resharding broadcast handled successfully'}), 200
  else:
    return json.dumps({'message': 'I am alive!'}), 200

# Replica View Routes
@api.route('/key-value-store-view', methods = ['GET', 'PUT', 'DELETE'])
def view():
  if request.method == 'GET':
    return get_view()
  elif request.method == 'PUT':
    return put_view(request.json.get('socket-address'))
  elif request.method == 'DELETE':
    return delete_view(request.json.get('socket-address'))

def get_view():
  return json.dumps({'message': 'View retrieved successfully', 'view': replica_store}), 200

def put_view(socket_addr):
  if socket_addr in replica_store:
    return json.dumps({'error': 'Socket address already exists in the view', 'message': 'Error in PUT'}), 404
  else:
    replica_store.append(socket_addr)
    return json.dumps({'message': 'Replica added successfully to the view'}), 201

def delete_view(socket_addr):
  if socket_addr in replica_store:
    replica_store.remove(socket_addr)
    return json.dumps({'message': 'Replica deleted successfully from the view'}), 200
  else:
    return json.dumps({'error': 'Socket address does not exist in the view', 'message': 'Error in DELETE'}), 404


# Shard Operation Routes
@api.route('/key-value-store-shard/<shard_op>', methods=['GET', 'PUT'])
def handle_shard_request(shard_op):
  if shard_op == 'shard-ids':
    return json.dumps({'message': 'Shard IDs retrieved successfully', 'shard-ids': list(shard_store.keys())}), 200
  elif shard_op == 'node-shard-id':
    return json.dumps({'message': 'Shard ID of the node retrieved successfully', 'shard-id': this_shard_id}), 200
  elif shard_op == 'reshard':
    reshard_count = request.json.get('shard-count')
    if len(replica_store) / reshard_count < 2:
      return json.dumps({'message': 'Not enough nodes to provide fault tolerance with the given shard count!'}), 400
    else:
      reshard(reshard_count)
      return json.dumps({'message': 'Resharding done successfully'}), 200

# Shard Operation Routes with Shard Number
@api.route('/key-value-store-shard/<shard_op>/<shard_num>', methods=['GET', 'PUT'])
def handle_shard_request_with_num(shard_op, shard_num):
  shard_id = (int)(shard_num)
  if shard_op == 'add-member':
    if request.json.get('socket-address') == socket_addr:
      return json.dumps({'message': 'Adding new member skipped on reflexive replica...'})

    new_node_ip = request.json.get('socket-address')
    shard_store[shard_id].append(new_node_ip)

    if not request.remote_addr+':8085' in replica_store:
      broadcast_request('PUT', '/key-value-store-shard/add-member/' + shard_num, request.json)
      requests.put('http://' + new_node_ip + '/internal?update_shard_store', json={'shard-store': shard_store, 'shard-id': shard_id})
      requests.get('http://' + new_node_ip + '/internal?update_store')

    return json.dumps({'message': 'Node successfully added to shard!'}), 200
  elif shard_op == 'shard-id-key-count':
    if shard_id == this_shard_id:
      return json.dumps({'message': 'Key count of shard ID retrieved successfully', 'shard-id-key-count': len(store)}), 200
    else:
      shard_nodes = shard_store.get(shard_id)
      shard_in_node = shard_nodes[random.randint(0, len(shard_nodes)-1)]
      response = requests.get('http://' + shard_in_node + '/key-value-store-shard/shard-id-key-count/' + shard_num)
      return response.content, response.status_code
  elif shard_op == 'shard-id-members':
    return json.dumps({'message': 'Members of shard ID retrieved successfully', 'shard-id-members': shard_store.get(shard_id)}), 200

# Key-Value Routes
@api.route('/key-value-store/<key>', methods=['GET', 'PUT', 'DELETE'])
def handle_KV_request(key):
  global vector_clock
  target_shard_id = key_to_shard_id(key)
  target_shards = shard_store.get(target_shard_id)
  node_in_shard = target_shards[random.randint(0, len(target_shards)-1)]

  if request.method == 'GET':
    if target_shard_id == this_shard_id:
      return get_key(key)
    else: # Forward to first replica in appropriate shard id.
      forward_url = 'http://' + node_in_shard + '/key-value-store/'+ key
      response = requests.get(forward_url)
      return response.content, response.status_code
  else:
    sender_addr = request.remote_addr + ':8085' # hard-coded port number
    metadata = request.json.get('causal-metadata')

    if is_next_operation(metadata):
      if request.method == 'PUT':
        if sender_addr not in replica_store:
          if target_shard_id == this_shard_id: # Broadcast if request is from client to the correct shard
            vector_clock = get_incremented_clock(vector_clock, socket_addr)
            broadcast_request('PUT', '/key-value-store/' + key, request.json)
            return put_key(key, request)
          else: # Forward to first replica in appropriate shard
            forward_url = 'http://' + node_in_shard + '/key-value-store/'+ key
            response = requests.put(forward_url, json=request.json)
            vector_clock = get_incremented_clock(vector_clock, node_in_shard)
            broadcast_request('PUT', '/internal?increment', {'forwarded-address': node_in_shard}, True)
            return response.content, response.status_code
        else:
          if target_shard_id == this_shard_id: # Received broadcasted request and modifying locally
            if sender_addr not in shard_store.get(target_shard_id):
              vector_clock = get_incremented_clock(vector_clock, socket_addr)
              broadcast_request('PUT', '/key-value-store/' + key, request.json, True)
            else:
              vector_clock = get_incremented_clock(vector_clock, sender_addr)
            return put_key(key, request)
          else:
            vector_clock = get_incremented_clock(vector_clock, sender_addr)
            return json.dumps({'message': 'Updated vector clock...'}), 203

      elif request.method == 'DELETE':
        if sender_addr not in replica_store:
          if target_shard_id == this_shard_id: # Broadcast if the request is from client to the correct shard
            broadcast_request('DELETE', '/key-value-store/' + key, request.json)
            return delete_key(key, request)
          else: # Forward to first replica in appropriate shard
            forward_url = 'http://' + node_in_shard + '/key-value-store/'+ key
            response = requests.delete(forward_url)
            broadcast_request('DELETE', '/internal?increment', {'forwarded-address': node_in_shard}, True)
            return response.content, response.status_code
        else:
          if target_shard_id == this_shard_id: # Received broadcasted reqest and modifying locally
            return delete_key(key, request)
          else:
            return json.dumps({'message': 'Updated vector clock...'}), 203

    else:
      queue.append({'key': key, 'request': request.json, 'method': request.method})
      vector_clock_for_client = get_incremented_clock(metadata, socket_addr)
      return json.dumps({'message': 'Request is queued, please wait...'}), 202

def get_key(key):
  if key in store:
    return json.dumps({'doesExist': True, 'causal-metadata': vector_clock, 'message': 'Retrieved successfully', 'value': store[key]}), 200
  else:
    return json.dumps({'doesExist': False, 'causal-metadata': vector_clock, 'error': 'Key does not exist', 'message': 'Error in GET'}), 404

def put_key(key, request):
  value = request.json.get('value')
  if value is None:
    return json.dumps({'error': 'Value is missing', 'message': 'Error in PUT'}), 400
  elif len(key) > 50:
    return json.dumps({'error': 'Key is too long', 'message': 'Error in PUT'}), 400

  if store.get(key) is None:
    store[key] = value
    check_queue()
    return json.dumps({'message': 'Added successfully', 'replaced': False, 'causal-metadata': vector_clock, 'shard-id': this_shard_id}), 201
  else:
    store[key] = value
    check_queue()
    return json.dumps({'message': 'Updated successfully', 'replaced': True, 'causal-metadata': vector_clock, 'shard-id': this_shard_id}), 200

def delete_key(key, request):
  global vector_clock
  if key in store:
    del store[key]
    check_queue()
    vector_clock = get_incremented_clock(vector_clock, socket_addr)
    return json.dumps({'message': 'Deleted successfully', 'doesExist': True, 'causal-metadata': vector_clock}), 200
  else:
    check_queue()
    vector_clock = get_incremented_clock(vector_clock, socket_addr)
    return json.dumps({'message': 'Error in DELETE', 'doesExist': False, 'error': 'Key does not exist', 'causal-metadata': vector_clock}), 404

# Determines whether some causal metadata corresponds with the next sequential operation
def is_next_operation(causal_metadata):
  if len(causal_metadata) == 0:
    return True
  else:
    return is_causally_independent(causal_metadata)

# Determines if some causal metadata correspond with a causally independent operation
def is_causally_independent(causal_metadata):
  if socket_addr in vector_clock and socket_addr in causal_metadata and vector_clock[socket_addr] != causal_metadata[socket_addr]:
      return False
  else:
    for key in causal_metadata:
      if not key in vector_clock or causal_metadata[key] > vector_clock[key]:  # Existent key > non-existent key
        return False
    return True

# Hashes a key to its corresponding shard ID
def key_to_shard_id(key):
  hash_value = md5(key.encode('utf-8'))
  key_value = int(hash_value.hexdigest(), 16)
  shard_id = key_value % len(shard_store.keys())
  return shard_id

def reshard(shard_count):
  os.environ['SHARD_COUNT'] = str(shard_count)
  old_shard_store = shard_store.copy()
  old_store = store.copy()

  initialize_shard()
  json_body = { 'shard-count': shard_count }
  broadcast_request('PUT', '/internal?reshard', json_body)

  # Reallocates KV-pairs no longer belonging to this shard
  for key in old_store:
    proper_shard = key_to_shard_id(key)
    if proper_shard != this_shard_id:
      proper_addr = shard_store[proper_shard][0]
      json_body = {'key': key, 'value': store[key]}
      requests.put('http://' + proper_addr  + '/internal?put_key', json=json_body)

      del store[key]
      for shard_id in shard_store:
        if shard_id != this_shard_id and shard_id != proper_shard:
          replica_addr =  shard_store[shard_id][0]
          requests.delete('http://' + replica_addr + '/internal?delete_key', json={'key': key})
    else:
      for shard_id in shard_store:
        if shard_id != this_shard_id:
          replica_addr =  shard_store[shard_id][0]
          requests.delete('http://' + replica_addr + '/internal?delete_key', json={'key': key})

  for shard_id in old_shard_store:
    if shard_id != this_shard_id: # Reallocates KV-pairs from all other shards
      replica_addr = old_shard_store[shard_id][0]
      response = requests.get('http://' + replica_addr + '/internal?store')
      some_store = response.json().get('store')

      for key in some_store:
        proper_shard = key_to_shard_id(key)
        if proper_shard != shard_id:
          if proper_shard == this_shard_id:
            store[key] = some_store[key]

            for some_shard_id in shard_store:
              if some_shard_id != this_shard_id:
                replica_addr =  shard_store[some_shard_id][0]
                requests.delete('http://' + replica_addr + '/internal?delete_key', json={'key': key})
          else:
            proper_replica = shard_store[proper_shard][0]
            json_body = {'key': key, 'value': some_store[key]}
            requests.put('http://' + proper_replica  + '/internal?put_key', json=json_body)

            for other_shard_id in shard_store:
              if other_shard_id != proper_shard:
                replica_addr =  shard_store[other_shard_id][0]
                requests.delete('http://' + replica_addr + '/internal?delete_key', json={'key': key})
        else:
          for another_shard_id in shard_store:
            if another_shard_id != shard_id:
              replica_addr =  shard_store[another_shard_id][0]
              requests.delete('http://' + replica_addr + '/internal?delete_key', json={'key': key})

  for last_shard_id in shard_store:
    first_replica = shard_store[last_shard_id][0]
    response = requests.get('http://' + first_replica + '/internal?store')
    forwarding_store = response.json().get('store')

    for another_replica in shard_store[last_shard_id]:
      if another_replica != first_replica:
        json_body = {'store': forwarding_store}
        requests.put('http://' + another_replica  + '/internal?put_store', json=json_body)

# Queries a replica's queue and processes all sequential operations
def check_queue():
  global vector_clock
  if len(queue) == 0:
    return

  for i in range(0, len(queue)):
    message = queue[i]
    if is_causally_independent(message.get('request').get('causal-metadata')):
      if message.get('method') == 'PUT':
        store[message.get('key')] = message.get('request').get('value')
      elif message.get('method') == 'DELETE':
        del store[message.get('key')]
      broadcast_request(message.get('method'), '/key-value-store/' + key, message.get('request'))

      vector_clock = get_incremented_clock(message.get('request').get('causal-metadata'), socket_addr)
      queue.pop(i)
    else:
      break

# Calculates an incremented vector clock based on some replica address
def get_incremented_clock(vector_clock, replica_addr):
  resulting_vector_clock = {}

  if len(vector_clock) == 0:
    resulting_vector_clock[replica_addr] = 1
  else:
    resulting_vector_clock = vector_clock.copy()
    if replica_addr not in resulting_vector_clock:
      resulting_vector_clock[replica_addr] = 1
    else:
      resulting_vector_clock[replica_addr] += 1
  return resulting_vector_clock

if __name__ == '__main__':
  initialize_view()
  initialize_shard()

  # polling_replica_thread = threading.Thread(target=poll_replicas)
  # polling_replica_thread.start()
  # # # polling_replica_thread.join() # This won't execute because thread is infinite, so it'll never end.

  # polling_vector_clock_thread = threading.Thread(target=poll_vector_clock)
  # polling_vector_clock_thread.start()
  # # # polling_vector_clock.join() # This won't execute because thread is infinite, so it'll never end.

  api.run(host='0.0.0.0', port=8085, debug=True, use_reloader=False)