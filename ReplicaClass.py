import random
import requests
import time
from flask import request
from threading import Thread

def monitor_online_status(replica):
    time.sleep(2)
    down_connections = 0
    while True:
        view = replica.view.copy()
        for ip_address, port in view.items():
            if ip_address != replica.ip_address:
                try:
                    requests.get("http://" + ip_address + ":" + port + "/", timeout=(2, 2))
                    if replica.is_down:
                        #replica.set_kv_store()
                        replica.broadcast_put_view(broadcast_delete=False)
                        down_connections = 0
                        replica.is_down = False
                except OSError:
                    down_connections += 1
        if down_connections >= len(view) - 1 and len(view) != 1:
            replica.is_down = True
        else:
            down_connections = 0
        time.sleep(1)


class Replica:
    def __init__(self, ip_address, port, view_string):
        self.ip_address = ip_address
        self.port = port
        self.view = {}
        self.set_view(view_string)
        self.vector_clock = dict.fromkeys(self.view, 0)
        self.kv_store = {}
        self.buffer = []
        self.is_down = False
        self.broadcast_put_view(broadcast_delete=False)
        Thread(target=monitor_online_status, args=[self]).start()
        

    # ------------------------------ Set ------------------------------ #

    def set_view(self, view_string):
        self.view = {}
        for pair in [pair.split(':') for pair in view_string.split(',')]:
            self.view[pair[0]] = pair[1]


    def increment_vector_clock(self):
        self.vector_clock[self.ip_address] += 1


    def set_max_vector_clock(self, sender_vc):
        for ip_address in self.vector_clock:
            self.vector_clock[ip_address] = max(self.vector_clock[ip_address], sender_vc[ip_address])


    # ------------------------------ Get ------------------------------ #

    def get_causal_md(self):
        return dict(sender_ip=self.ip_address, sender_vc=self.vector_clock)


    def get_view_as_string(self):
        string = ""
        for ip_address, port in self.view.items():
            string += str(ip_address) + ':' + str(port)
            string += ","
        return string[:-1]

    # ------------------------------ Broadcast ------------------------------ #

    def broadcast_put_key_value(self, key, value):
        for ip_address, port in self.view.copy().items():
            if ip_address != self.ip_address:
                try:
                    requests.put("http://" + ip_address + ":" + port + "/key-value-store/" + key,
                                 json={"value": value, "causal-metadata": self.get_causal_md()}, timeout=(2, 4))
                except:
                    self.view.pop(ip_address)
                    self.vector_clock.pop(ip_address)
                    self.broadcast_delete_view(ip_address, port)


    def broadcast_delete_key_value(self, key):
        for ip_address, port in self.view.copy().items():
            if ip_address != self.ip_address:
                try:
                    requests.delete("http://" + ip_address + ":" + port + "/key-value-store/" + key,
                                    json={"causal-metadata": self.get_causal_md()}, timeout=(2, 4))
                except:
                    self.view.pop(ip_address)
                    self.vector_clock.pop(ip_address)
                    self.broadcast_delete_view(ip_address, port)


    def broadcast_put_view(self, broadcast_delete=True):
        for ip_address, port in self.view.copy().items():
            if ip_address != self.ip_address:
                try:
                    requests.put("http://" + ip_address + ":" + port + "/key-value-store-view",
                                 json={"socket-address": self.ip_address + ":" + self.port}, timeout=(2, 4))
                except:
                    if broadcast_delete:
                        self.view.pop(ip_address)
                        self.vector_clock.pop(ip_address)
                        self.broadcast_delete_view(ip_address, port)


    def broadcast_delete_view(self, del_address, del_port):
        for ip_address, port in self.view.copy().items():
            if ip_address != self.ip_address and ip_address != del_address:
                try:
                    requests.delete("http://" + ip_address + ":" + port + "/key-value-store-view",
                                    json={"socket-address": del_address + ":" + del_port}, timeout=(2, 4))
                except:
                    continue


    # ------------------------------ Delivery ------------------------------ #

    def deliverable(self, sender_ip, sender_vc):
        if sender_vc is None and sender_ip is None:
            return True

        isNextMessage = False
        if self.vector_clock[sender_ip] + 1 == sender_vc[sender_ip] or self.vector_clock == sender_vc:
            isNextMessage = True

        noMissingMessages = True
        for ip_address in self.vector_clock:
            if ip_address != sender_ip:
                if self.vector_clock[ip_address] < sender_vc[ip_address]:
                    noMissingMessages = False
                    break

        return isNextMessage and noMissingMessages


    def add_to_buffer(self, sender_request):
        self.buffer.append(sender_request)


    def process_buffer(self):
        if len(self.buffer) == 0:
            return
        self.deliver(self.buffer.pop(0))


    def deliver(self, request):
        if request.method == 'GET':
            requests.get(request.url, request.json, timeout=(2, 4))
        elif request.method == 'PUT':
            requests.put(request.url, request.json, timeout=(2, 4))
        else:
            requests.delete(request.url, request.json, timeout=(2, 4))
