import os
import time
import json
import requests
from flask import Flask, request, jsonify
from threading import Thread
from ShardClass import Shard

# ------------------------------ Init ------------------------------ #

shard = Shard(os.environ.get('SOCKET_ADDRESS').split(':')[0], os.environ.get('SOCKET_ADDRESS').split(':')[1],
                  os.environ.get("VIEW"), os.environ.get('SHARD_COUNT'))

broadcast_thread, buffer_thread = Thread(), Thread()


# ------------------------------ Route General ------------------------------ #

app = Flask(__name__)

@app.before_request
def sleep_when_down():
    while shard.is_down:
        time.sleep(1)

@app.route("/", methods=["GET"])
def ping():
    if request.method == 'GET':
        return '', 200


# ------------------------------ Route Shard ------------------------------ #

@app.route("/key-value-store-shard/member-list", methods=["GET", "PUT"])
def member_list():
    if request.method == 'GET':
        return jsonify(shard.members), 200
    if request.method == 'PUT':
        shard.id = request.get_json(force=True).get('shard-id')
        shard.members = request.get_json(force=True).get('members-list')
        shard.hash_seed = request.get_json(force=True).get('hash-seed')
        shard.count = len(shard.members)
        shard.vector_clock = dict.fromkeys(shard.view, 0)
        return '', 200


@app.route("/key-value-store-shard/shard-ids", methods=["GET"])
def shard_ids():
    if request.method == 'GET':
        return jsonify({"message": "Shard IDs retrieved successfully", "shard-ids": shard.get_shard_ids_as_string()}), 200


@app.route("/key-value-store-shard/node-shard-id", methods=["GET"])
def node_id():
    if request.method == 'GET':
        return jsonify({"message": "Shard ID of the node retrieved successfully", "shard-id": shard.id}), 200


@app.route("/key-value-store-shard/shard-id-members/<shard_id>", methods=["GET"])
def id_members(shard_id):
    if request.method == 'GET':
        return jsonify({"message": "Members of shard ID retrieved successfully",
                        "shard-id-members": shard.get_members_as_string(shard_id)}), 200


@app.route("/key-value-store-shard/shard-id-key-count/<shard_id>", methods=["GET"])
def key_count(shard_id):
    if request.method == 'GET':
        return jsonify({"message":"Key count of shard ID retrieved successfully",
                        "shard-id-key-count": shard.get_num_keys(shard_id)}), 200


@app.route("/key-value-store-shard/add-member/<shard_id>", methods=["PUT"])
def add_member(shard_id):
    if request.method == 'PUT':
        ip_address, port = request.get_json(force=True).get('socket-address').split(':')
        members_list = request.get_json(force=True).get('members-list', None)
        #TODO might need to change conditional if shard that was newly instantiated is the one getting this request
        if shard.members is None and members_list is not None:
            shard.id = shard_id
            shard.members = members_list
            shard.count = len(shard.members)
            shard.set_kv_store()
        elif shard.members is not None:
            shard.add_shard_member(shard_id, ip_address, port)
        if request.remote_addr not in shard.view:
            shard.broadcast_add_member(shard_id, ip_address, port)
        return '', 200


@app.route("/key-value-store-shard/reshard", methods=["PUT"])
def reshard():
    if request.method == 'PUT':
        shard_count = int(request.get_json(force=True).get('shard-count'))
        if len(shard.view)/shard_count < 2:
            return jsonify({"message":"Not enough nodes to provide fault-tolerance with the given shard count!"}), 400
        shard.reshard(shard_count)
        return jsonify({"message":"Resharding done successfully"}), 200


# ------------------------------ Route View ------------------------------ #

@app.route("/key-value-store-view", methods=["GET", "PUT", "DELETE"])
def view():
    if request.method == 'GET':
        return jsonify(
            {"message": "View retrieved successfully", "view": "{}".format(shard.get_view_as_string())}), 200

    if request.method == 'PUT':
        ip_address, port = request.get_json(force=True).get('socket-address').split(':')
        if ip_address not in shard.view:
            shard.view[ip_address] = port
            shard.vector_clock[ip_address] = 0
            return jsonify({"message": "Replica added successfully to the view"}), 201
        else:
            return jsonify({"error": "Socket address already exists in the view", "message": "Error in PUT"}), 404

    if request.method == 'DELETE':
        ip_address, port = request.get_json(force=True).get('socket-address').split(':')
        if shard.view.pop(ip_address, None) != None:
            shard.vector_clock.pop(ip_address)
            return jsonify({"message": "Replica deleted successfully from the view"}), 200
        else:
            return jsonify({"error": "Socket address does not exist in the view", "message": "Error in DELETE"}), 404


# ------------------------------ Route Key Value Store ------------------------------ #

@app.route("/key-value-store", methods=["GET", "PUT"])
def kv_store():
    if request.method == 'GET':
        return jsonify(shard.kv_store), 200
    if request.method == 'PUT':
        shard.kv_store = request.get_json(force=True).get('kv-store')
        return '', 200


@app.route("/key-value-store/<key>", methods=["GET", "PUT", "DELETE"])
def kv_store_key(key):
    global broadcast_thread
    global buffer_thread

    if request.method == 'GET':
        # Send request to the shard the key hashes to
        key_shard_id = shard.calculate_shard_id(key)
        if key_shard_id != shard.id:
            for ip_address, port in shard.members[key_shard_id].items():
                try:
                    url = "http://" + ip_address + ":" + port + "/key-value-store/" + key
                    response = requests.get(url)
                    return response.json(), response.status_code
                except:
                    continue

        if key in shard.kv_store:
            return jsonify(
                {"doesExist": True, "message": "Retrieved successfully", "value": shard.kv_store[key]["value"],
                 "causal-metadata": shard.kv_store[key]["causal-metadata"]}), 200
        else:
            return jsonify({"doesExist": False, "error": "Key does not exist", "message": "Error in GET"}), 404

    if request.method == 'PUT':
        value = request.get_json(force=True).get('value')
        if value is None:
            return jsonify({"error": "Value is missing", "message": "Error in PUT"}), 400
        elif len(key) > 50:
            return jsonify({"error": "Key is too long", "message": "Error in PUT"}), 400
        else:
            # Send request to the shard the key hashes to
            key_shard_id = shard.calculate_shard_id(key)
            if key_shard_id != shard.id:
                for ip_address, port in shard.members[key_shard_id].items():
                    try:
                        url = "http://" + ip_address + ":" + port + "/key-value-store/" + key
                        response = requests.put(url, request.get_data())
                        return response.json(), response.status_code
                    except:
                        continue

            # Get causal metadata
            sender_md = request.get_json(force=True).get('causal-metadata')
            if type(sender_md) is not dict: sender_md = {}

            # Check if deliverable
            if shard.deliverable(sender_md.get('sender_ip'), sender_md.get('sender_vc')):
                if key not in shard.kv_store:
                    shard.kv_store[key] = {}

                # Update replica's vector clock to the max of the received vector clock.
                if sender_md:  # non-empty
                    shard.set_max_vector_clock(sender_md.get('sender_vc'))

                if request.remote_addr not in shard.members[shard.id]:
                    # Update vector clock and metadata
                    shard.increment_vector_clock()
                    shard.kv_store[key]["causal-metadata"] = shard.get_causal_md()

                    # Broadcast PUT to other replicas
                    if broadcast_thread.is_alive(): broadcast_thread.join()
                    broadcast_thread = Thread(target=shard.broadcast_put_key_value, args=(key, value))
                    broadcast_thread.start()
                else:
                    shard.kv_store[key]["causal-metadata"] = sender_md

                # Process buffer
                if buffer_thread.is_alive(): buffer_thread.join()
                buffer_thread = Thread(target=shard.process_buffer)
                buffer_thread.start()

                # Update local storage and respond to client
                if "value" not in shard.kv_store[key]:
                    shard.kv_store[key]["value"] = value
                    return jsonify({"message": "Added successfully", "replaced": False,
                                    "causal-metadata": shard.kv_store[key]["causal-metadata"],
                                    "shard-id": shard.id}), 201
                else:
                    shard.kv_store[key]["value"] = value
                    return jsonify({"message": "Updated successfully", "replaced": True,
                                    "causal-metadata": shard.kv_store[key]["causal-metadata"],
                                    "shard-id": shard.id}), 200

            else:  # Request not deliverable
                shard.add_to_buffer(request)

    if request.method == 'DELETE':
        # Send request to the shard the key hashes to
        key_shard_id = shard.calculate_shard_id(key)
        if key_shard_id != shard.id:
            for ip_address, port in shard.members[key_shard_id].items():
                try:
                    url = "http://" + ip_address + ":" + port + "/key-value-store/" + key
                    response = requests.delete(url=url, data=request.get_data())
                    return response.json(), response.status_code
                except:
                    continue

        if key not in shard.kv_store:
            return jsonify({"doesExist": False, "error": "Key does not exist", "message": "Error in DELETE"}), 404

        # Get causal metadata
        sender_md = request.get_json(force=True).get('causal-metadata')
        if type(sender_md) is not dict: sender_md = {}

        # Check if deliverable
        if shard.deliverable(sender_md.get('sender_ip'), sender_md.get('sender_vc')):
            # Update replica's vector clock to the max of the received vector clock.
            if sender_md:  # non-empty
                shard.set_max_vector_clock(sender_md.get('sender_vc'))

            if request.remote_addr not in shard.members[shard.id]:
                # Update vector clock and metadata
                shard.increment_vector_clock()
                shard.kv_store[key]["causal-metadata"] = shard.get_causal_md()

                # Broadcast DELETE to other replicas
                if broadcast_thread.is_alive(): broadcast_thread.join()
                broadcast_thread = Thread(target=shard.broadcast_delete_key_value, args=(key,))
                broadcast_thread.start()

            # Process buffer
            if buffer_thread.is_alive(): buffer_thread.join()
            buffer_thread = Thread(target=shard.process_buffer)
            buffer_thread.start()

        else:  # Request not deliverable
            shard.add_to_buffer(request)

        if shard.kv_store.pop(key, None) != None:
            return jsonify(
                {"doesExist": True, "message": "Deleted successfully", "causal-metadata": shard.get_causal_md(),
                 "shard-id": shard.id}), 200

app.run(host='0.0.0.0', port=8085, debug=True, use_reloader=False)
