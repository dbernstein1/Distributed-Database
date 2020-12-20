import hashlib
import random
import requests
from ReplicaClass import Replica

class Shard(Replica):
    def __init__(self, ip_address, port, view_string, count=None):
        super().__init__(ip_address, port, view_string)
        self.hash_seed = 1
        self.id = None
        self.members = None
        if count is not None:
            self.count = int(count)
            self.members = self.init_members(self.view)
            self.id = self.calculate_shard_id(self.ip_address)


    def get_shard_ids_as_string(self):
        return list(self.members.keys())

    def get_members_as_string(self, shard_id):
        member_list = []
        for ip_address, port in self.members[shard_id].items():
            string = str(ip_address) + ':' + str(port)
            member_list.append(string)
        return member_list

    def get_members_list(self, shard_id):
        for ip_address, port in random.sample(self.members[shard_id].items(), len(self.members[shard_id])):
            try:
                return requests.get("http://" + ip_address + ":" + port +
                                    "/key-value-store-shard/shard-id-key-count/" + shard_id,
                                    timeout=(2, 4)).json()["shard-id-key-count"]
            except:
                continue

    def init_members(self, view):
        members = {}
        # Set members
        for node in view.keys():
            shard_id = self.calculate_shard_id(node)
            if shard_id not in members:
                members[shard_id] = {}
            members[shard_id][node] = view[node]
        # Check that there are the correct number of shards
        if len(members) != self.count:
            self.hash_seed += 1
            return self.init_members(view)
        # Check that each shard has at least two nodes
        for shard_id in members: 
            if len(members[shard_id]) < 2:
                self.hash_seed += 1
                return self.init_members(view)
        return members

    # Sets kv_store to that of another node with the same shard-id
    def set_kv_store(self):
        for ip_address, port in self.members[self.id].items():
            if ip_address != self.ip_address:
                try:
                    self.kv_store = requests.get("http://" + ip_address + ":" + port + "/key-value-store",
                                                 timeout=(2, 4)).json()
                    break
                except:
                    continue


    def add_shard_member(self, shard_id, ip_address, port):
        self.members[shard_id].update({ip_address:port})

    def remove_shard_member(self, shard_id, ip_address):
        self.members[shard_id].pop(ip_address)


    def get_num_keys(self, shard_id):
        if self.id == shard_id:
            return len(self.kv_store)
        else:
            for ip_address, port in self.members[shard_id].items():
                try:
                    return requests.get("http://" + ip_address + ":" + port +
                                        "/key-value-store-shard/shard-id-key-count/" + shard_id,
                                        timeout=(2, 4)).json()["shard-id-key-count"]
                except:
                    continue


    def broadcast_put_reshard(self, shard_count):
        for ip_address, port in self.view.copy().items():
            if ip_address != self.ip_address:
                try:
                    requests.put("http://" + ip_address + ":" + port + "/key-value-store-shard/reshard",
                                 json={"shard-count" : shard_count}, timeout=(2, 4))
                except:
                    self.view.pop(ip_address)
                    self.vector_clock.pop(ip_address)
                    self.broadcast_delete_view(ip_address, port)

    def broadcast_add_member(self, shard_id, new_ip_address, new_port):
        for ip_address, port in self.view.copy().items():
            if ip_address != self.ip_address:
                try:
                    requests.put("http://" + ip_address + ":" + port +
                                 "/key-value-store-shard/add-member/" + shard_id,
                                 json={"socket-address": new_ip_address + ":" + new_port,
                                       "members-list" : self.members}, timeout=(2, 4))
                except:
                    self.view.pop(ip_address)
                    self.vector_clock.pop(ip_address)
                    self.broadcast_delete_view(ip_address, port)

    #PUT broadcast for all nodes in the same Shard
    def broadcast_put_key_value(self, key, value):
        for ip_address, port in self.members[self.id].copy().items():
            if ip_address != self.ip_address:
                try:
                    requests.put("http://" + ip_address + ":" + port + "/key-value-store/" + key,
                                 json={"value": value, "causal-metadata": self.get_causal_md()}, timeout=(2, 4))
                except:
                    self.view.pop(ip_address)
                    self.vector_clock.pop(ip_address)
                    self.broadcast_delete_view(ip_address, port)

    #DELETE Broadcast for all nodes in the same Shard
    def broadcast_delete_key_value(self, key):
        for ip_address, port in self.members[self.id].copy().items():
            if ip_address != self.ip_address:
                try:
                    requests.delete("http://" + ip_address + ":" + port + "/key-value-store/" + key,
                                    json={"causal-metadata": self.get_causal_md()}, timeout=(2, 4))
                except:
                    self.view.pop(ip_address)
                    self.vector_clock.pop(ip_address)
                    self.broadcast_delete_view(ip_address, port)


    def deliverable(self, sender_ip, sender_vc):
        if (sender_vc is None and sender_ip is None) or sender_ip not in self.members[self.id]:
            return True

        isNextMessage = False
        if self.vector_clock[sender_ip] + 1 == sender_vc[sender_ip] or self.vector_clock == sender_vc:
            isNextMessage = True

        noMissingMessages = True
        for ip_address in self.members[self.id].keys():
            if ip_address != sender_ip:
                if self.vector_clock[ip_address] + 1 < sender_vc[ip_address]:
                    noMissingMessages = False
                    break

        return isNextMessage and noMissingMessages


    def reshard(self, shard_count):
        self.count = shard_count

        # Get kv_store from all shards
        all_kv_stores = {}
        for shard_id in self.members:
            for ip_address, port in self.members[shard_id].items():
                if ip_address != self.ip_address:
                    try:
                        kv_store = requests.get("http://" + ip_address + ":" + port + "/key-value-store",
                                                 timeout=(2, 4)).json()
                        for key in kv_store:
                            kv_store[key]["causal-metadata"] = {}
                        all_kv_stores.update(kv_store)
                        break
                    except:
                        continue

        self.members = self.init_members(self.view)
        self.id = self.calculate_shard_id(self.ip_address)

        # Create updated_kv_store from hashing all_kv_stores with new shard-count
        updated_kv_store = {}
        for key, value in all_kv_stores.items():
            shard_id = self.calculate_shard_id(key)
            if shard_id not in updated_kv_store:
                updated_kv_store[shard_id] = {}
            updated_kv_store[shard_id].update({key:value})

        self.kv_store = updated_kv_store[self.id]

        # Broadcast to all other nodes
        for shard_id in self.members:
            for ip_address, port in self.members[shard_id].items():
                if ip_address != self.ip_address:
                    try:
                        # Broadcast new members list
                        requests.put("http://" + ip_address + ":" + port + "/key-value-store-shard/member-list",
                                     json={"shard-id": shard_id, "members-list": self.members, "hash-seed": self.hash_seed}, timeout=(2, 4))
                        # Broadcast new kv_store
                        requests.put("http://" + ip_address + ":" + port + "/key-value-store",
                                        json={"kv-store": updated_kv_store[shard_id]}, timeout=(2, 4))
                    except:
                        self.view.pop(ip_address)
                        self.vector_clock.pop(ip_address)
                        self.broadcast_delete_view(ip_address, port)

        return

    #not sure if this is works (since shard-ids is a string) or is nessesary 
    #def get_count(self):
    #    for ip_address, port in random.sample(self.view.items(), len(self.view):
    #                try:
    #                    return len(requests.get("http://" + ip_address + ":" + port +
    #                                        "/key-value-store-shard/shard-ids",
    #                                        timeout=(2, 4)).json()["shard-ids"])
    #                except:
    #                    continue
    
    ''' calculate_shard_id: 
    1. determines shard ID during init
    2. A node receiving a forwarded GET request recalculates the shard
        of <key> to ensure that it has received the correct request.
    '''
    def calculate_shard_id(self, key):
        key += str(self.hash_seed)
        h = hashlib.md5(key.encode())
        return str(int(h.hexdigest(), 16) % self.count)
