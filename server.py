
from hashlib import md5
from flask import Flask, json, request, jsonify
import os, sys, requests, threading, time

api = Flask(__name__)

store = {}

replica_store = []
vector_clock = {}
queue = []

shard_store = {}
this_shard_id = None

socket_addr = os.environ.get('SOCKET_ADDRESS')
default_view_str = os.environ.get('VIEW')
default_view = default_view_str.split(',')

# Initialization Method
def initialize_view():
  global replica_store
  replica_store = default_view
  json_body = { 'socket-address': socket_addr }
  broadcast_request('PUT', '/key-value-store-view', json_body) # Weird bug - two PUT requests are sent?

def initialize_shard():
  # Don't initialize shard store if SHARD_COUNT env var is none,
  # Which can happen if it's a newly added node.
  shard_count_env = os.environ.get('SHARD_COUNT')
  if (shard_count_env is None):
    return

  global shard_store, this_shard_id
  for i in range(len(replica_store)):
    id = i % (int)(shard_count_env)
    this_replica = replica_store[i]

    if this_replica == socket_addr:
      this_shard_id = id
    shard_store.setdefault(id, []).append(this_replica)

# Polling Other Replicas
def poll_replicas():
  while True:
    global replica_store
    time.sleep(5)
    for replica_addr in default_view:
      if replica_addr != socket_addr:
        forward_url = 'http://' + replica_addr + '/key-value-store-view'
        try:
          response = requests.get(forward_url)
        # except (requests.exceptions.Timeout, requests.exceptions.ConnectionError, ConnectionRefusedError):
        except:
          delete_view(replica_addr)
          json_body = { 'socket-address': replica_addr }
          broadcast_request('DELETE', '/key-value-store-view', json_body)

def poll_vector_clock():
  while True:
    time.sleep(1)
    global vector_clock, store
    for replica_addr in default_view:
      if replica_addr != socket_addr:
        forward_url = 'http://' + replica_addr + '/key-value-store-view?clock'
        try:
          response = requests.get(forward_url, timeout=1)
          remote_vector_clock = response.json().get('vector_clock')
          if replica_addr in remote_vector_clock:
            if replica_addr not in vector_clock or remote_vector_clock[replica_addr] > vector_clock[replica_addr]:
              store = response.json().get('store')
              vector_clock = remote_vector_clock
              check_queue()
        except:
          pass

# Broadcast Message
def broadcast_request(request_type, target_endpoint, json_body=None, to_shard_replicas=False):
  local_store = None
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
      except Exception as e:
        pass

# Replica View Routes
@api.route('/key-value-store-view', methods = ['GET', 'PUT', 'DELETE'])
def view():
  global store, vector_clock
  if request.method == 'GET':
    return get_view(request.args)
  elif request.method == 'PUT':
    if 'replace_store' in request.args:
      store = request.json.get('store')
    elif 'increment' in request.args:
      vector_clock = get_incremented_clock(vector_clock, request.json.get('forwarded-address'))
    else:
      return put_view(request.json.get('socket-address'))
  elif request.method == 'DELETE':
    return delete_view(request.json.get('socket-address'))

def get_view(params):
  global replica_store
  if 'store' in params:
    return json.dumps({'message': 'Store retrieved successfully', 'store': store}), 200
  elif 'clock' in params:
    return json.dumps({'vector_clock': vector_clock, 'store': store}), 200
  else:
    return json.dumps({'message': 'View retrieved successfully', 'view': replica_store}), 200

def put_view(socket_addr):
  global replica_store
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
  global shard_store
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

@api.route('/key-value-store-shard/<shard_op>/<shard_num>', methods=['GET', 'PUT'])
def handle_shard_request_with_num(shard_op, shard_num):
  global shard_store
  shard_id = (int)(shard_num)
  if shard_op == 'add-member':
    new_node_ip = request.json.get('socket-address')
    shard_store[shard_id].append(new_node_ip)
    broadcast_request('PUT', '/internal/add-member', {'new-node-ip': new_node_ip, 'shard-id': shard_id})
    requests.put('http://' + new_node_ip + '/internal/catch-up', json={'shard-store': shard_store})
    return "Node added.", 200
  elif shard_op == 'shard-id-key-count':
    return json.dumps({'message': 'Key count of shard ID retrieved successfully', 'shard-id-key-count': len(store)}), 200
  elif shard_op == 'shard-id-members':
    if shard_id in shard_store.keys():
      return json.dumps({'message': 'Members of shard ID retrieved successfully', 'shard-id-members': shard_store.get(shard_id)}), 200

# Internal route that handles the broadcast of adding a new node.
@api.route('/internal/add-member', methods=['PUT'])
def handle_internal_add_member():
  new_node_ip = request.json.get('new-node-ip')
  # Skip if it's about self.
  if (new_node_ip == socket_addr):
    return "Skipped.", 200
  shard_id = request.json.get('shard-id')
  shard_store[shard_id].append(new_node_ip)
  return "Added.", 200

# Internal route for a new node to receive the latest shard store.
@api.route('/internal/catch-up', methods=['PUT'])
def handle_interal_catch_up():
  global store, shard_store, this_shard_id
  json_shard_store = request.json.get('shard-store')
  for shard_id_str, node_socks in json_shard_store.items():
    shard_id = (int)(shard_id_str)
    shard_store[shard_id] = node_socks
    # Get this_shard_id
    if socket_addr in node_socks:
      this_shard_id = shard_id
  # Get store from the right shard ID.
  # Not the cleanest. What if first node in the shard is the new node itself?
  node_from_same_shard = shard_store[this_shard_id][0]
  # Not clean. Why can store be got from view?
  url_to_get_store = 'http://' + node_from_same_shard + '/key-value-store-view?store'
  #print(requests.get(url_to_get_store), file=sys.stderr)
  store = requests.get(url_to_get_store).json().get('store')
  return "Updated.", 200

# Key-Value Routes
@api.route('/key-value-store/<key>', methods=['GET', 'PUT', 'DELETE'])
def handle_KV_request(key):
  global vector_clock
  global shard_store
  requestShardID = key_to_shard_id(key)
  findNodeInShard = shard_store.get(requestShardID)
  firstReplicaInShard = findNodeInShard[0]
  print(requestShardID, file=sys.stderr)
  if request.method == 'GET':
    if requestShardID == this_shard_id:
      return get_key(key)
    else:
      # Forward to first replica in appropriate shard id.
      # findNodeInShard = shard_store.get(requestShardID)
      # firstReplicaInShard = findNodeInShard[0]
      forwardUrl = 'http://' + firstReplicaInShard + '/key-value-store/'+ key
      response = requests.get(forwardUrl)
      return response.content, response.status_code
  sender_addr = request.remote_addr+':8085' # hard-coded port number
  metadata = request.json.get('causal-metadata')
  if is_next_operation(metadata):
    if request.method == 'PUT':
        # Should process
        if sender_addr not in replica_store:
          if requestShardID == this_shard_id:
          # Broadcast if the request is from client, and shard id matches.
            vector_clock = get_incremented_clock(vector_clock, socket_addr)
            broadcast_request('PUT', '/key-value-store/' + key, request.json)
            return put_key(key, request)
          else:
            # Forward to first replica in appropriate shard id.
            # findNodeInShard = shard_store.get(requestShardID)
            # firstReplicaInShard = findNodeInShard[0]
            forwardUrl = 'http://' + firstReplicaInShard + '/key-value-store/'+ key
            response = requests.put(forwardUrl, json = request.json)
            vector_clock = get_incremented_clock(vector_clock, firstReplicaInShard)
            broadcast_request('PUT', '/key-value-store-view?increment', {'forwarded-address': firstReplicaInShard}, True)
            return response.content, response.status_code
        else:
          # received broadcast/forwarded request.
          if requestShardID == this_shard_id:
            if sender_addr not in shard_store.get(requestShardID):
              vector_clock = get_incremented_clock(vector_clock, socket_addr)
              # received forwarded request - broadcast within shard
              broadcast_request('PUT', '/key-value-store/' + key, request.json, True)
            else:
              vector_clock = get_incremented_clock(vector_clock, sender_addr)
            return put_key(key, request)
          else:
            vector_clock = get_incremented_clock(vector_clock, sender_addr)
            return "updating vector clock only"
    elif request.method == 'DELETE':
      if sender_addr not in replica_store:
        if requestShardID == this_shard_id:
          # Broadcast if the request is from client.
          broadcast_request('DELETE', '/key-value-store/' + key, request.json)
          vector_clock = get_incremented_clock(vector_clock, socket_addr)
          return delete_key(key, request)
        else:
          # Forward to first replica in appropriate shard id.
          # findNodeInShard = shard_store.get(requestShardID)
          # firstReplicaInShard = findNodeInShard[0]
          forwardUrl = 'http://' + firstReplicaInShard + '/key-value-store/'+ key
          response = requests.delete(forwardUrl)
          vector_clock = get_incremented_clock(vector_clock, firstReplicaInShard)
          return response.content, response.status_code
      else:
        # received broadcast/forwarded request.
        if requestShardID == this_shard_id:
          if sender_addr not in shard_store.get(this_shard_id):
            vector_clock = get_incremented_clock(vector_clock, socket_addr)
            # received forwarded request - broadcast within shard
            broadcast_request('DELETE', '/key-value-store/' + key, request.json, True)
          else:
            vector_clock = get_incremented_clock(vector_clock, sender_addr)
          return delete_key(key, request)
        else:
          vector_clock = get_incremented_clock(vector_clock, sender_addr)
          return "updating vector clock only"
  # Should queue
  else:
    queue_request(key, request.json, request.method)
    vector_clock_for_client = get_incremented_clock(metadata, socket_addr)
    return json.dumps({'causal-metadata': vector_clock_for_client, 'message': 'Request is queued, please wait...'}), 202

def get_key(key):
  global vector_clock
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

  global vector_clock
  # Set response body based on if it's a adding or updating a key.
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
  response_body = {}
  if key in store:
    del store[key]
    check_queue()
    vector_clock = get_incremented_clock(vector_clock, socket_addr)
    return json.dumps({'message': 'Deleted successfully', 'doesExist': True, 'causal-metadata': vector_clock}), 200
  else:
    check_queue()
    vector_clock = get_incremented_clock(vector_clock, socket_addr)
    return json.dumps({'message': 'Error in DELETE', 'doesExist': False, 'error': 'Key does not exist', 'causal-metadata': vector_clock}), 404

def is_next_operation(metadata):
  if len(metadata) == 0:
    return True
  else:
    return is_causally_independent(metadata)

def is_causally_independent(metadata):
  if socket_addr in vector_clock and socket_addr in metadata:
    if vector_clock[socket_addr] != metadata[socket_addr]:
      return False
  for key in metadata:
    if not key in vector_clock:  # Existent key > non-existent key
      return False
    if metadata[key] > vector_clock[key]:
      return False

  return True

def key_to_shard_id(key):
  hash_value = md5(key.encode('utf-8'))
  key_value = int(hash_value.hexdigest(), 16)
  shard_id = key_value % len(shard_store.keys())
  return shard_id

def reshard(shard_count):
  global shard_store, replica_store, this_shard_id, store
  # make a copy of shard_store
  this_shard_id = shard_count
  shard_store_old = shard_store.copy()

  # initialize copied shard_store with new keys
  #   redistribute replicaIDs into new shardIDs

  for i in range(len(replica_store)):
    id = i % shard_count
    this_replica = replica_store[i]

    if this_replica == socket_addr:
      this_shard_id = id
    shard_store.setdefault(id, []).append(this_replica)

  # detect which have been added or removed from this shardID from 0 to len(shard_store)
  key_shard_map = {}

  # This adds all the keys to the respective shardID
  #Refactor: Can also request store from every repica in other shards
  for k in store.keys():
    id = key_to_shard_id(k)
    key_shard_map.setdefault(id, []).append((k, store[k]))

  #remove and populate all kvs of replicas in the shard

  for id in shard_store:
    #Creates a new store for the shard_id
    print("id: ", id)
    ListOfShardTuples = key_shard_map.get(id)
    print("ListOfShardTuples: ", ListOfShardTuples)
    store_to_be_added = {}
    for k in ListOfShardTuples:
      for a, b in k:
        store_to_be_added.set(a,b)
    json_body = { 'store': store_to_be_added }
    # Check if it is our shard, so we can change locally
    if id == this_shard_id:
      store = store_to_be_added
      broadcast_request('PUT', '/key-value-store?replace_store', json_body, True)
    else:
      #If it is not our replica
      replicaList = shard_store[id]
      request.put('http://' +replicaList[0]+'/key-store-view?replace_store', json_body)

  # for all the shards that been removed
  #   delete all key from this (current) shardID
          #   override current  keys from replicas with keys from new shardID
  # for all the shards that have added to this shardID
  #   copy current replica store and override on newly added replicas
  pass

def queue_request(key, req, method):
  queue.append(json.dumps({'key': key, 'request': req, 'method': method}))

def check_queue():
  global vector_clock, queue
  if len(queue) == 0:
    return
  else:
    while True:
      has_processed = False
      processed_req_idx = -1
      for i in range(0, len(queue)):
        objvector_clock  = json.loads(queue[i]).get('request').get('causal-metadata')
        if is_causally_independent(objvector_clock):

          # Read the request.
          msg = json.loads(queue[i])
          key = msg.get('key')
          req = msg.get('request')
          value = req.get('value')

          # Process the request.
          if msg.get('method') == 'PUT':
            store[key] = value
          elif msg.get('method') == 'DELETE':
            del store[key]

          # Broadcast the request.
          broadcast_request(msg.get('method'), '/key-value-store/' + key, req.get('json'))

          # Update stuff.
          vector_clock = get_incremented_clock(objvector_clock.copy(), socket_addr)
          has_processed = True
          processed_req_idx = i
          break

      if (has_processed):
        queue.pop(processed_req_idx)
      else:
        break

def get_incremented_clock(vector_clock, addr):
  result = {}
  if len(vector_clock) == 0:
    result[addr] = 1
    return result
  result = vector_clock.copy()
  if result.get(addr) is None:
    result[addr] = 1
  else:
    result[addr] += 1
  return result

if __name__ == '__main__':
  initialize_view()
  initialize_shard()
  # polling_replica_thread = threading.Thread(target=poll_replicas)
  # polling_replica_thread.start()
  # polling_replica_thread.join() # This won't execute because thread is infinite, so it'll never end.

  # polling_vector_clock_thread = threading.Thread(target=poll_vector_clock)
  # polling_vector_clock_thread.start()
  # polling_vector_clock.join() # This won't execute because thread is infinite, so it'll never end.

  api.run(host='0.0.0.0', port=8085, debug=True)

