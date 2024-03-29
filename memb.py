# Sample simulator demo
# Miguel Matos - miguel.marques.matos@tecnico.ulisboa.pt
# (c) 2012-2018
from __future__ import division

import ast
import csv
import gc
import hashlib

import datetime
import time
from collections import defaultdict
import random
import sys
import os

import yaml
import cPickle
import logging

from pip._vendor.ipaddress import IPv4Address

from sim import sim
import utils

# Messages structures

# Node structure
# NODES_TO_RELAY - cheat to avoid using a more specialized function
# TODO update total_nodes
CURRENT_CYCLE, CURRENT_TIME, KEY, RANDOM, NODE_NEIGHBOURHOOD, NODES_CONNECTED, TRIED_NEW, NODES_TO_RELAY, TRIED_COLLISIONS, MSGS \
    = 0, 1, 2, 3, 4, 5, 6, 7, 8, 9

N_TRIED, TRIED, N_NEW, NEW = 0, 1, 2, 3

# Node neighbour structure
F_INBOUND, F_IN_TRIED, F_SHOULD_BAN, SOURCE, TIME, PING_STRC, ADDR_STRC, MISBEHAVIOR, REF_COUNT, LAST_TRY, ATTEMPTS, LAST_SUCCESS\
    = 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11

# Addr structure
F_SENT_ADDR, F_GET_ADDR, NEXT_ADDR_SEND, NEXT_LOCAL_ADDR_SEND, ADDR_TO_SEND, ADDR_KNOWN = 0, 1, 2, 3, 4, 5

AVG_LOCAL_ADDRESS_BROADCAST_INTERVAL = 24 * 60 * 60

AVG_ADDRESS_BROADCAST_INTERVAL = 30

MAX_ADDR_MSG_SIZE = 1000

BANSCORE = 100

ADDRMAN_GETADDR_MAX_PCT = 23

ADDRMAN_GETADDR_MAX = 2500

ADDRMAN_TRIED_BUCKET_COUNT = 256

ADDRMAN_TRIED_BUCKETS_PER_GROUP = 8

ADDRMAN_NEW_BUCKET_COUNT = 1024

ADDRMAN_NEW_BUCKETS_PER_SOURCE_GROUP = 64

ADDRMAN_BUCKET_SIZE = 64

ADDRMAN_NEW_BUCKETS_PER_ADDRESS = 8

ADDRMAN_SET_TRIED_COLLISION_SIZE = 10

# Addr message struct
ADDR_ID, ADDR_TIME = 0, 1

# Ping structure
PING_NONCE_SENT, PING_TIME_START, PING_TIME, BEST_PING_TIME = 0, 1, 2, 3

PING_INTERVAL = 2 * 60

# Messages sent
VERSION_MSG, VERACK_MSG, GET_ADDR_MSG, ADDR_MSG, PING_MSG, PONG_MSG = 0, 1, 2, 3, 4, 5

# Log intervals
INTERVAL = 18000


def init():
    # schedule execution for all nodes
    for nodeId in nodeState:
        sim.schedulleExecution(CYCLE, nodeId)

    # other things such as periodic measurements can also be scheduled
    # to schedule periodic measurements use the following
    # for c in range(nbCycles * 10):
    #    sim.scheduleExecutionFixed(MEASURE_X, nodeCycle * (c + 1))


def improve_performance(cycle):
    if cycle % 600 != 0 or cycle == 0:
        return

    gc.collect()
    if gc.garbage:
        gc.garbage[0].set_next(None)
        del gc.garbage[:]


def connect_node(myself, addr_connect):
    if addr_connect not in nodeState[myself][NODE_NEIGHBOURHOOD]:
        raise ValueError("Smth is off")
    pto = nodeState[myself][NODE_NEIGHBOURHOOD][addr_connect]
    pto[REF_COUNT] += 1


def initialize_node(myself, addr_connect, inbound):
    pto = nodeState[myself][NODE_NEIGHBOURHOOD][addr_connect]
    pto[F_INBOUND] = inbound
    if pto[F_INBOUND]:
        sim.send(VERSION, addr_connect, myself)


def open_network_connection(myself, addr_connect, feeler, inbound):
    connect_node(myself, addr_connect)
    initialize_node(myself, addr_connect, inbound)


def get_chance(myself, id):
    now = nodeState[myself][CURRENT_TIME]
    pto = nodeState[myself][NODE_NEIGHBOURHOOD][id]
    chance = 1.0
    since_last_try = max(now - pto[LAST_TRY], 0)

    if since_last_try < 60 * 10:
        chance *= 0.01

    chance *= pow(0.66, min(pto[ATTEMPTS], 8))
    return chance


def select(myself, new_only):

    new = nodeState[myself][TRIED_NEW][NEW]
    n_new = nodeState[myself][TRIED_NEW][N_NEW]
    tried = nodeState[myself][TRIED_NEW][TRIED]
    n_tried = nodeState[myself][TRIED_NEW][N_TRIED]

    if len(nodeState[myself][RANDOM]) == 0:
        raise ValueError("TODO implement")

    if new_only and n_new == 0:
        raise ValueError("TODO implement")

    chance_factor = 1.0
    if not new_only and n_tried > 0 and (n_new == 0 or random.randint(2) == 0):
        while True:
            k_bucket = random.randint(ADDRMAN_TRIED_BUCKET_COUNT)
            k_bucket_pos = random.randint(ADDRMAN_BUCKET_SIZE)
            while tried[k_bucket][k_bucket_pos] == -1:
                k_bucket = (k_bucket + random.getrandbits(8)) % ADDRMAN_TRIED_BUCKET_COUNT
                k_bucket_pos = (k_bucket_pos + random.getrandbits(6)) % ADDRMAN_BUCKET_SIZE
            id = tried[k_bucket][k_bucket_pos]
            if random.randint(1073741824) < chance_factor * get_chance(myself, id) * 1073741824:
                return id
            chance_factor *= 1.2
    else:
        u_bucket = random.randint(0, ADDRMAN_NEW_BUCKET_COUNT-1)
        u_bucket_pos = random.randint(0, ADDRMAN_BUCKET_SIZE-1)
        while new[u_bucket][u_bucket_pos] == -1:
            u_bucket = (u_bucket + random.getrandbits(10)) % ADDRMAN_NEW_BUCKET_COUNT
            u_bucket_pos = (u_bucket_pos + random.getrandbits(6)) % ADDRMAN_BUCKET_SIZE
        id = new[u_bucket][u_bucket_pos]
        if random.randint(0, 1073741824) < chance_factor * get_chance(myself, id) * 1073741824:
            return id
        chance_factor *= 1.2


def open_connections(myself):
    # TODO REMOVE ONCE FEELER IMPLEMENTED TEMPORARY
    counter = 0
    for node in nodeState[myself][NODES_CONNECTED]:
        if not nodeState[myself][NODE_NEIGHBOURHOOD][node][F_INBOUND]:
            counter += 1

    if counter >= 8:
        return
    # _____________________________

    feeler = False
    addr_connect = None
    # resolve_collisions(myself)
    now = nodeState[myself][CURRENT_TIME]
    tries = 0
    while True:
        # addr = select_tried_colision(myself)
        addr = None
        if addr is None and not feeler:
            addr = select(myself, feeler)
            # pto = nodeState[myself][NODE_NEIGHBOURHOOD][addr[ADDR_ID]]

        tries += 1
        if tries > 100:
            break

        # TODO would be nice to have
        # if now - pto[LAST_TRY] < 600 and tries < 30:
        #   continue

        addr_connect = addr
        break

    #if not feeler:
    #    raise ValueError("TO implement")

    open_network_connection(myself, addr_connect, feeler, False)


def CYCLE(myself):
    global nodeState

    # with churn the node might be gone
    if myself not in nodeState:
        return

    # show progress for one node
    if myself == 0 and nodeState[myself][CURRENT_CYCLE] % 43200 == 0:
        improve_performance(nodeState[myself][CURRENT_CYCLE])
        value = datetime.datetime.fromtimestamp(time.time())
        #output.write('{} id: {} cycle: {}\n'.format(value.strftime('%Y-%m-%d %H:%M:%S'), runId, nodeState[myself][CURRENT_CYCLE]))
        #output.flush()
        print('{} id: {} cycle: {}'.format(value.strftime('%Y-%m-%d %H:%M:%S'), runId, nodeState[myself][CURRENT_CYCLE]))

    # Cheat to avoid using a deterministic function to know which nodes will we send addr's
    if nodeState[myself][CURRENT_CYCLE] % (24 * 60 * 60) == 0:
        update_relays(myself)

    # Check if we need to make a dns request
    dns_address_seed(myself)

    open_connections(myself)
    # Main cycle to dispatch messages on nodes which we are connected to
    for node in nodeState[myself][NODES_CONNECTED]:
        cnode = nodeState[myself][NODE_NEIGHBOURHOOD][node]
        send_messages(myself, node, cnode)

    # TODO implement feeler connections

    nodeState[myself][CURRENT_CYCLE] += 1
    nodeState[myself][CURRENT_TIME] += 1
    # schedule next execution
    if nodeState[myself][CURRENT_CYCLE] < nb_cycles:
        sim.schedulleExecution(CYCLE, myself)


def VERSION(myself, source):
    global nodeState

    if should_log(myself):
        nodeState[myself][MSGS][VERSION_MSG] += 1

    # Node connects to itself case, there's a condition in the Bitcoin code for this
    if myself == source:
        return

    if source not in nodeState[myself][NODE_NEIGHBOURHOOD]:
        add_new_addr(myself, source, source, 0)
        open_network_connection(myself, source, False)

    pto = nodeState[myself][NODE_NEIGHBOURHOOD][source]

    if pto[F_INBOUND]:
        sim.send(VERSION, source, myself)

    sim.send(VERACK, source, myself)

    if not pto[F_INBOUND]:
        addr = get_addr(myself)
        push_address(pto, addr)

        if len(nodeState[myself][RANDOM]) < 1000:
            sim.send(GETADDR, source, myself)
            pto[ADDR_STRC][F_GET_ADDR] = True

        mark_address_as_good(myself, source, True, nodeState[myself][CURRENT_TIME])

    # TODO implement feeler

    return True


def VERACK(myself, source):
    global nodeState

    # TODO prevent nodes from starting communications with this message in case that could happen

    if should_log(myself):
        nodeState[myself][MSGS][VERACK_MSG] += 1

    pto = nodeState[myself][NODE_NEIGHBOURHOOD][source]
    nodeState[myself][NODES_CONNECTED].append(source)


def GETADDR(myself, source):
    global nodeState

    check_if_connected(myself, source)

    if should_log(myself):
        nodeState[myself][MSGS][GET_ADDR_MSG] += 1

    pto = nodeState[myself][NODE_NEIGHBOURHOOD][source]
    if not pto[F_INBOUND]:
        return

    if pto[ADDR_STRC][F_SENT_ADDR]:
        return

    pto[ADDR_STRC][F_SENT_ADDR] = True

    pto[ADDR_STRC][ADDR_TO_SEND] = []

    n_nodes = ADDRMAN_GETADDR_MAX_PCT * len(nodeState[myself][RANDOM]) / 100
    if n_nodes > ADDRMAN_GETADDR_MAX:
        n_nodes = ADDRMAN_GETADDR_MAX

    v_addr = []
    for n in range(0, len(nodeState[myself][RANDOM])):
        if len(v_addr) > n_nodes:
            break
        n_rad_pos = random.randint(len(nodeState[myself][RANDOM]) - n) + n
        swap_random(myself, n, n_rad_pos)
        addr = get_addr(myself, nodeState[myself][RANDOM][n])
        if not is_terrible(myself, addr[ADDR_ID]):
            v_addr.append(addr)
        n += 1

    for addr in v_addr:
        push_address(pto, addr)


def ADDR(myself, source, addrs):
    global nodeState

    check_if_connected(myself, source)

    now = nodeState[myself][CURRENT_TIME]
    pto = nodeState[myself][NODE_NEIGHBOURHOOD][source]

    if should_log(myself):
        nodeState[myself][MSGS][ADDR_MSG] += 1

    if len(addrs) > 1000:
        misbehaving(myself, source, 20)
        return

    addr_ok = []
    since = now - 10 * 60
    for addr in addrs:
        if not may_have_useful_address_db(addr):
            continue
        if addr[ADDR_TIME] > now + 10 * 60:
            addr[ADDR_TIME] = now - 5 * 24 * 60 * 60
        add_addr(pto, addr)
        if addr[ADDR_TIME] > since and not pto[ADDR_STRC][F_GET_ADDR] and len(addrs) <= 10:
            relay_address(myself, addr)

        # TODO implement is reachable
        addr_ok.append(addr)

    add_new_addresses(myself, source, addr_ok, 2 * 60 * 60)
    if len(addrs) < 1000:
        pto[ADDR_STRC][F_GET_ADDR] = False

    # TODO implement one shot


def PING(myself, source, nonce):
    global nodeState

    check_if_connected(myself, source)

    if should_log(myself):
        nodeState[myself][MSGS][PING_MSG] += 1

    sim.send(PONG, source, myself, nonce)


def PONG(myself, source, nonce):
    global nodeState

    check_if_connected(myself, source)

    if should_log(myself):
        nodeState[myself][MSGS][PONG_MSG] += 1

    pfrom = nodeState[myself][NODE_NEIGHBOURHOOD][source]
    ping_time_end = nodeState[myself][CURRENT_TIME]
    b_ping_finished = False

    if pfrom[PING_STRC][PING_NONCE_SENT] != 0:
        if nonce == pfrom[PING_STRC][PING_NONCE_SENT]:
            b_ping_finished = True
            ping_time = ping_time_end - pfrom[PING_STRC][PING_TIME_START]
            if ping_time > 0:
                pfrom[PING_STRC][PING_TIME] = ping_time
                pfrom[PING_STRC][BEST_PING_TIME] = min(pfrom[PING_STRC][BEST_PING_TIME], ping_time)
            else:
                raise ValueError("This should never happen")
        else:
            if nonce == 0:
                b_ping_finished = True

    if b_ping_finished:
        pfrom[PING_STRC][PING_NONCE_SENT] = 0


def check_if_connected(myself, source):
    if source not in nodeState[myself][NODES_CONNECTED]:
        raise ValueError("Node not connected trying to communicate")


def make_tried(myself, source):
    new = nodeState[myself][TRIED_NEW][NEW]
    for bucket in range(0, ADDRMAN_NEW_BUCKET_COUNT):
        pos = get_bucket_position(nodeState[myself][KEY], True, bucket, source)
        if new[bucket][pos] == source:
            new[bucket][pos] = -1
            nodeState[myself][TRIED_NEW][N_NEW] -= 1
            nodeState[myself][NODE_NEIGHBOURHOOD][source][REF_COUNT] -= 1

    if nodeState[myself][NODE_NEIGHBOURHOOD][source][REF_COUNT] != 0:
        raise ValueError("This should be 0")

    k_bucket = get_tried_bucket(nodeState[myself][KEY], source)
    k_bucket_pos = get_bucket_position(nodeState[myself][KEY], False, k_bucket, source)
    tried = nodeState[myself][TRIED_NEW][TRIED]
    if tried[k_bucket][k_bucket_pos] != -1:
        id_evict = tried[k_bucket][k_bucket_pos]
        old_pto = nodeState[myself][NODE_NEIGHBOURHOOD][id_evict]
        old_pto[F_IN_TRIED] = False
        tried[k_bucket][k_bucket_pos] = -1
        nodeState[myself][TRIED_NEW][N_TRIED] -= 1

        u_bucket = get_new_bucket(nodeState[myself][KEY], source, nodeState[myself][NODE_NEIGHBOURHOOD][source][SOURCE])
        u_bucket_pos = get_bucket_position(nodeState[myself][KEY], True, u_bucket, source)
        clear_new(myself, u_bucket, u_bucket_pos)

        old_pto[REF_COUNT] = 1
        new[u_bucket][u_bucket_pos] = id_evict
        nodeState[myself][TRIED_NEW][N_NEW] += 1

    tried[k_bucket][k_bucket_pos] = source
    nodeState[myself][TRIED_NEW][N_TRIED] += 1
    pto = nodeState[myself][NODE_NEIGHBOURHOOD][source]
    pto[F_IN_TRIED] = True


def mark_address_as_good(myself, source, test_before_evict, time):

    if source not in nodeState[myself][NODE_NEIGHBOURHOOD]:
        return

    pto = nodeState[myself][NODE_NEIGHBOURHOOD][source]
    pto[LAST_SUCCESS] = time
    pto[LAST_TRY] = time
    pto[ATTEMPTS] = 0

    if pto[F_IN_TRIED]:
        return

    rnd = random.randint(ADDRMAN_NEW_BUCKET_COUNT)
    u_bucket = -1
    for n in range(0, ADDRMAN_NEW_BUCKET_COUNT):
        b = (n + rnd) % ADDRMAN_NEW_BUCKET_COUNT
        b_pos = get_bucket_position(nodeState[myself][KEY], True, b, source)
        new = nodeState[myself][TRIED_NEW][NEW]
        if new[b][b_pos] == source:
            u_bucket = b

    if u_bucket == -1:
        raise ValueError("This shouldn't be happening")

    tried_bucket = get_tried_bucket(nodeState[myself][KEY], source)
    tried_bucket_pos = get_bucket_position(nodeState[myself][KEY], False, tried_bucket, source)
    tried = nodeState[myself][TRIED_NEW][TRIED]
    if test_before_evict and tried[tried_bucket][tried_bucket_pos] != -1:
        if len(nodeState[myself][TRIED_COLLISIONS]) < ADDRMAN_SET_TRIED_COLLISION_SIZE:
            nodeState[myself][TRIED_COLLISIONS].append(source)
    else:
        make_tried(myself, source)


# --------------------------------------
# Addr functions
def add_addr(pto, addr):
    if addr not in pto[ADDR_STRC][ADDR_KNOWN]:
        pto[ADDR_STRC][ADDR_KNOWN][addr[ADDR_ID]] = addr[ADDR_TIME]


def has_addr(pto, addr):
    if addr in pto[ADDR_STRC][ADDR_KNOWN]:
        return True
    return False


def get_addr(myself, n=None):
    if n is None:
        return [myself, nodeState[myself][CURRENT_TIME]]
    cnode = nodeState[myself][NODE_NEIGHBOURHOOD][n]
    return [n, cnode[TIME]]


def push_address(pto, addr):
    if len(addr) != 2:
        raise ValueError("Size of the addr message invalid")

    if not has_addr(pto, addr):
        if len(pto[ADDR_STRC][ADDR_TO_SEND]) >= MAX_ADDR_MSG_SIZE:
            i = random.randint(len(pto[ADDR_STRC][ADDR_TO_SEND]))
            pto[ADDR_STRC][ADDR_TO_SEND][i] = addr
        else:
            pto[ADDR_STRC][ADDR_TO_SEND].append(addr)


def add_new_addresses(myself, source, addr_ok, time_penalty):
    for addr in addr_ok:
        add_new_addr(myself, source, addr, time_penalty)


def delete(myself, id):
    if id in nodeState[myself][NODE_NEIGHBOURHOOD]:
        pto = nodeState[myself][NODE_NEIGHBOURHOOD][id]
        if not pto[F_IN_TRIED] and pto[REF_COUNT] == 0:
            nodeState[myself][RANDOM].remove(id)
            del nodeState[myself][NODE_NEIGHBOURHOOD][id]
            nodeState[myself][TRIED_NEW][N_NEW] -= 1


def add_new_addr(myself, source, addr, time_penalty):
    f_new = False
    now = nodeState[myself][CURRENT_TIME]
    pto = None
    if addr[ADDR_ID] in nodeState[myself][NODE_NEIGHBOURHOOD]:
        pto = nodeState[myself][NODE_NEIGHBOURHOOD][addr[ADDR_ID]]

    if addr[ADDR_ID] == source:
        time_penalty = 0

    if pto is not None:
        f_currently_online = (now - addr[ADDR_TIME] < 24 * 60 * 60)
        update_interval = 60 * 60 if f_currently_online else 24 * 60 * 60
        if addr[ADDR_TIME] and (not pto[TIME] or pto[TIME] < addr[ADDR_TIME] - update_interval - time_penalty):
            pto[TIME] = max(0, addr[ADDR_TIME] - time_penalty)

        if not addr[ADDR_TIME] or (pto[TIME] and addr[ADDR_TIME] <= pto[TIME]):
            return False

        if pto[F_IN_TRIED]:
            return False

        if pto[REF_COUNT] == ADDRMAN_NEW_BUCKETS_PER_ADDRESS:
            return False

        n_factor = 1
        n = 0
        while n < pto[REF_COUNT]:
            n_factor *= 2
            n += 1
        if n_factor > 1 and random.randint(0, n_factor) != 0:
            return False
    else:
        create_neighbour_structures(myself, addr[ADDR_ID], True, source)
        pto = nodeState[myself][NODE_NEIGHBOURHOOD][addr[ADDR_ID]]
        pto[TIME] = max(0, pto[TIME] - time_penalty)
        f_new = True

    u_bucket = get_new_bucket(nodeState[myself][KEY], addr, source)
    u_bucket_pos = get_bucket_position(nodeState[myself][KEY], True, u_bucket, addr)
    vv_new = nodeState[myself][TRIED_NEW][NEW]
    if vv_new[u_bucket][u_bucket_pos] != addr[ADDR_ID]:
        insert = vv_new[u_bucket][u_bucket_pos] == -1
        if not insert:
            cnode = nodeState[myself][NODE_NEIGHBOURHOOD][vv_new[u_bucket][u_bucket_pos]]
            if is_terrible(myself, vv_new[u_bucket][u_bucket_pos]) or cnode[REF_COUNT] > 1 and pto[REF_COUNT] == 0:
                insert = True

        if insert:
            clear_new(myself, u_bucket, u_bucket_pos)
            pto[REF_COUNT] += 1
            vv_new[u_bucket][u_bucket_pos] = addr[ADDR_ID]
            nodeState[myself][TRIED_NEW][N_NEW] += 1
        else:
            if pto[REF_COUNT] == 0:
                delete(myself, addr[ADDR_ID])

    return f_new


def get_new_bucket(key, id, source):
    if isinstance(source, str):
        group = get_group(source)
    else:
        group = get_group(get_IP(source))
    to_hash = str(key) + get_group(get_IP(id[ADDR_ID])) + group
    hash1 = (hashlib.sha256(to_hash)).hexdigest()
    to_hash = str(key) + group + str(int(hash1, 16) % ADDRMAN_NEW_BUCKETS_PER_SOURCE_GROUP)
    hash2 = (hashlib.sha256(to_hash)).hexdigest()
    return int(hash2, 16) % ADDRMAN_NEW_BUCKET_COUNT


def get_tried_bucket(key, id):
    to_hash = str(key) + get_IP(id[ADDR_ID])
    hash1 = (hashlib.sha256(to_hash)).hexdigest()
    to_hash = str(key) + get_group(get_IP(id[ADDR_ID])) + str(int(hash1, 16) % ADDRMAN_TRIED_BUCKETS_PER_GROUP)
    hash2 = (hashlib.sha256(to_hash)).hexdigest()
    return int(hash2, 16) % ADDRMAN_TRIED_BUCKET_COUNT


def get_bucket_position(key, new, bucket, addr):
    to_hash = str(key) + ('N' if new else 'K') + str(bucket) + str(addr[ADDR_ID])
    hash1 = (hashlib.sha256(to_hash)).hexdigest()
    return int(hash1, 16) % ADDRMAN_BUCKET_SIZE


def clear_new(myself, u_bucket, u_bucket_pos):
    new = nodeState[myself][TRIED_NEW][NEW]
    if new[u_bucket][u_bucket_pos] != -1:
        id_delete = new[u_bucket][u_bucket_pos]
        pto = nodeState[myself][NODE_NEIGHBOURHOOD][id_delete]
        pto[REF_COUNT] -= 1
        new[u_bucket][u_bucket_pos] = -1
        # TODO delete if ...
        # if pto[REF_COUNT] == 0:


def dns_address_seed(myself):
    if len(nodeState[myself][RANDOM]) > 0:
        if not nodeState[myself][CURRENT_TIME] % 11 == 0:
            return

        relevant = 0
        for node in nodeState[myself][NODES_CONNECTED]:
            pto = nodeState[myself][NODE_NEIGHBOURHOOD][node]
            relevant += not pto[F_INBOUND]

        if relevant >= 2:
            return

    i = 0
    for dns in dns_seeds:
        for addr in dns:
            one_day = 3600 * 24
            addr_copy = list(addr)
            addr_copy[ADDR_TIME] = nodeState[myself][CURRENT_TIME] - (3 * one_day) - random.randint(0, 4) * one_day
            add_new_addr(myself, dns_seeds_ip[i], addr_copy, 0)
        i += 1

# --------------------------------------


# --------------------------------------
# Broadcast functions
def poisson_next_send(now, avg_inc):
        return now + avg_inc * random.randrange(1, 5)


def relay_address(myself, addr):
    # TODO implement limited relay with reachable
    n_relay_nodes = 2

    # TODO might have to change
    if addr not in nodeState[myself][NODES_TO_RELAY]:
        nodeState[myself][NODES_TO_RELAY][addr] = random.sample(nodeState[myself][NODES_CONNECTED], n_relay_nodes)

    for node in nodeState[myself][NODES_TO_RELAY][addr]:
        pto = nodeState[myself][NODE_NEIGHBOURHOOD][node]
        push_address(pto, addr)


def update_relays(myself):
    for addr in nodeState[myself][NODES_TO_RELAY]:
        nodeState[myself][NODES_TO_RELAY][addr] = random.sample(nodeState[myself][NODES_CONNECTED], 2)
# --------------------------------------


# --------------------------------------
# Misc functions
def get_IP(id):
    return ip[id]


def get_group(ip):
    new_str = ip.split(".")
    return new_str[1] + "." + new_str[2]


def may_have_useful_address_db(addr):
    return


def swap_random(myself, n, n_rad_pos):
    addr_1 = nodeState[myself][RANDOM][n]
    addr_2 = nodeState[myself][RANDOM][n_rad_pos]
    nodeState[myself][RANDOM][n] = addr_2
    nodeState[myself][RANDOM][n_rad_pos] = addr_1


# Neighbours quality
def misbehaving(myself, source, how_much):
    if how_much == 0:
        return

    pto = nodeState[myself][NODE_NEIGHBOURHOOD][source]
    pto[MISBEHAVIOR] += how_much
    if pto[MISBEHAVIOR] - how_much < BANSCORE <= pto[MISBEHAVIOR]:
        pto[F_SHOULD_BAN] = True
    return


def is_terrible(myself, id):
    pto = nodeState[myself][NODE_NEIGHBOURHOOD][id]
    now = nodeState[myself][CURRENT_TIME]
    if pto[LAST_TRY] and pto[LAST_TRY] >= now - 60:
        return False

    if pto[TIME] > now + 10 * 60:
        return True

    if pto[TIME] == 0 or now - pto[TIME] > 30 * 24 * 60 * 60:
        return True

    if pto[LAST_SUCCESS] == 0 and pto[ATTEMPTS] >= 3:
        return True

    if now - pto[LAST_SUCCESS] > 7 * 24 * 60 * 60 and pto[ATTEMPTS] >= 10:
        return True

    return False


# --------------------------------------
# Cycle functions
def send_messages(myself, target, pto):
    send_ping(myself, target, pto)
    send_reject_and_check_if_banned(myself, target, pto)
    address_refresh_broadcast(myself, pto)
    send_addr(myself, target, pto)


def send_ping(myself, target, pto):
    global ping_nonce

    ping_send = False
    if pto[PING_STRC][PING_NONCE_SENT] == 0 and pto[PING_STRC][PING_TIME_START] + PING_INTERVAL < nodeState[myself][CURRENT_TIME]:
        ping_send = True
    if ping_send:
        pto[PING_STRC][PING_TIME_START] = nodeState[myself][CURRENT_TIME]
        pto[PING_STRC][PING_NONCE_SENT] = ping_nonce
        ping_nonce += 1
        sim.send(PING, target, myself, pto[PING_STRC][PING_NONCE_SENT])


def send_reject_and_check_if_banned(myself, target, pto):
    # TODO implement
    return


def address_refresh_broadcast(myself, pto):
    if pto[ADDR_STRC][NEXT_LOCAL_ADDR_SEND] < nodeState[myself][CURRENT_TIME]:
        push_address(pto, [myself, get_IP(myself), nodeState[myself][CURRENT_TIME]])
        pto[ADDR_STRC][NEXT_LOCAL_ADDR_SEND] = poisson_next_send(nodeState[myself][CURRENT_TIME], AVG_LOCAL_ADDRESS_BROADCAST_INTERVAL)


def send_addr(myself, target, pto):
    if pto[ADDR_STRC][NEXT_ADDR_SEND] < nodeState[myself][CURRENT_TIME]:
        pto[ADDR_STRC][NEXT_ADDR_SEND] = poisson_next_send(nodeState[myself][CURRENT_TIME], AVG_ADDRESS_BROADCAST_INTERVAL)
        addr_to_send = []
        for addr in pto[ADDR_STRC][ADDR_TO_SEND]:
            if not has_addr(pto, addr):
                add_addr(pto, addr)
                addr_to_send.append(addr)
                if len(addr_to_send) >= MAX_ADDR_MSG_SIZE:
                    sim.send(ADDR, target, myself, addr_to_send)
        pto[ADDR_STRC][ADDR_TO_SEND] = []
        if addr_to_send:
            sim.send(ADDR, target, myself, addr_to_send)
# --------------------------------------


def should_log(myself):
    if (expert_log and INTERVAL < nodeState[myself][CURRENT_CYCLE] < nb_cycles - INTERVAL) or not expert_log:
        return True
    return False


# --------------------------------------
# Start up functions
def create_network(create_new_network, save_network_conn, filename=""):

    first_time = not os.path.exists("networks/")
    network_first_time = not os.path.exists("networks/" + filename)

    if first_time:
        os.makedirs("networks/")

    if network_first_time or create_new_network:
        create_nodes()
        if save_network_conn:
            save_network()
    else:
        load_network(filename)
    create_bad_node()


def save_network():
    with open('networks/{}'.format(nb_nodes), 'w') as file_to_write:
        file_to_write.write("{}\n".format(nb_nodes))
        for n in xrange(nb_nodes):
            file_to_write.write(str(nodeState[n][NODE_NEIGHBOURHOOD]) + '\n')


def load_network(filename):
    global nodeState, nb_nodes

    if filename == "":
        raise ValueError("No file named inputted in not create new run")

    with open('networks/' + filename, 'r') as file_to_read:
        first_line = file_to_read.readline()
        nb_nodes = int(first_line.split()[0])
        nodeState = defaultdict()
        for n in xrange(nb_nodes):
            nodeState[n] = create_node()


def create_node():
    current_cycle = 0
    current_time = base_time
    key = random.getrandbits(256)
    msgs = [0] * 6
    neighbourhood_dic = defaultdict()

    vv_new = []
    for i in range(0, 1024):
        vv_new.append([-1] * 64)

    vv_tried = []
    for i in range(0, 256):
        vv_tried.append([-1] * 64)

    tried_new = [0, vv_tried, 0, vv_new]
    nodes_to_relay = defaultdict()
    tried_collisions = []
    neighbourhood = []
    random_vec = []
    return [current_cycle, current_time, key, random_vec, neighbourhood_dic, neighbourhood, tried_new, nodes_to_relay, tried_collisions, msgs]


def create_neighbour(inbound, source):
    in_tried = False
    should_ban = False
    time = 0
    ping = [0, 0, 0, 0]
    addr = [False, 0, 0, [], []]
    misbehaviour = 0
    ref_count = 0
    last_try = 0
    attempts = 0
    last_success = 0
    return [inbound, in_tried, should_ban, source, time, ping, addr, misbehaviour, ref_count, last_try, attempts, last_success]


def create_neighbour_structures(myself, id, inbound, source):
    global nodeState

    # No point in doing this if the node already exists
    if id in nodeState[myself][NODE_NEIGHBOURHOOD]:
        return

    if len(nodeState[myself][NODES_CONNECTED]) == 125:
        raise ValueError("Number of connections in one node exceed the maximum allowed")

    nodeState[myself][NODE_NEIGHBOURHOOD][id] = create_neighbour(inbound, source)
    nodeState[myself][RANDOM].append(id)


def create_nodes():
    global nodeState

    nodeState = defaultdict()
    for n in xrange(nb_nodes):
        nodeState[n] = create_node()


def create_bad_node():
    global bad_nodes

    bad_nodes = random.sample(xrange(nb_nodes), int((number_of_bad_nodes/100) * nb_nodes))


def create_ips():
    global ip

    for i in range(0, nb_nodes):
        addr_str = gen_ip()
        while addr_str in ip:
            addr_str = gen_ip()
        ip.append(addr_str)


def gen_ip():
    bits = random.getrandbits(32)
    addr = IPv4Address(bits)
    return str(addr)


def create_DNS():
    global dns_seeds, dns_seeds_ip

    for i in range(0, 7):
        list_of_nodes = random.sample(xrange(nb_nodes), 256)
        to_append = []
        for node in list_of_nodes:
            time = random.randint(691200, 1382400)
            to_append.append([node, time])
        dns_seeds.append(to_append)

        addr_str = gen_ip()
        while addr_str in ip:
            addr_str = gen_ip()
        dns_seeds_ip.append(addr_str)


def configure(config):
    global nb_nodes, nb_cycles, nodeState, node_cycle, expert_log, bad_nodes, number_of_bad_nodes, ping_nonce, ip, dns_seeds, dns_seeds_ip, base_time

    node_cycle = int(config['NODE_CYCLE'])

    nb_nodes = config['NUMBER_OF_NODES']
    max_outbound_nodes = int(config['MAX_OUTBOUND_NODES'])
    if number_of_bad_nodes == 0:
        number_of_bad_nodes = int(config['NUMBER_OF_BAD_NODES'])

    nb_cycles = config['NUMBER_OF_CYCLES']

    expert_log = bool(config['EXPERT_LOG'])
    if expert_log and nb_cycles < INTERVAL:
        raise ValueError("You have to complete more than {} cycles if you have expert_log on".format(INTERVAL))

    # Initializing globals
    ping_nonce = 0
    ip = []
    dns_seeds = []
    dns_seeds_ip = []
    base_time = config['BASE_TIME']

    create_ips()
    create_DNS()
    create_network(create_new, save_network_connections, file_name)

    IS_CHURN = config.get('CHURN', False)
    if IS_CHURN:
        CHURN_RATE = config.get('CHURN_RATE', 0.)
    MESSAGE_LOSS = float(config.get('MESSASE_LOSS', 0))
    if MESSAGE_LOSS > 0:
        sim.setMessageLoss(MESSAGE_LOSS)

    nodeDrift = int(nb_cycles * float(config['NODE_DRIFT']))
    latencyTablePath = config['LATENCY_TABLE']
    latencyValue = None

    try:
        with open(latencyTablePath, 'r') as f:
            latencyTable = cPickle.load(f)
    except:
        latencyTable = None
        latencyValue = int(latencyTablePath)
        logger.warn('Using constant latency value: {}'.format(latencyValue))

    latencyTable = utils.check_latency_nodes(latencyTable, nb_nodes, latencyValue)
    latencyDrift = eval(config['LATENCY_DRIFT'])

    sim.init(node_cycle, nodeDrift, latencyTable, latencyDrift)
# --------------------------------------


# --------------------------------------
# Wrap up functions
def wrapup():
    global nodeState
    version_messages = map(lambda x: nodeState[x][MSGS][VERSION_MSG], nodeState)
    verack_messages = map(lambda x: nodeState[x][MSGS][VERACK_MSG], nodeState)
    get_addr_messages = map(lambda x: nodeState[x][MSGS][GET_ADDR_MSG], nodeState)
    addr_messages = map(lambda x: nodeState[x][MSGS][ADDR_MSG], nodeState)
    ping_messages = map(lambda x: nodeState[x][MSGS][PING_MSG], nodeState)
    pong_messages = map(lambda x: nodeState[x][MSGS][PONG_MSG], nodeState)
    utils.dump_as_gnu_plot([version_messages, verack_messages, get_addr_messages, addr_messages, ping_messages, pong_messages],
                           dumpPath + '/messages-' + str(runId) + '.gpData', ['version verack get_addr addr ping pong'])

    first_time = not os.path.isfile('out/{}.csv'.format(results_name))
    if first_time:
        csv_file_to_write = open('out/results.csv', 'w')
        spam_writer = csv.writer(csv_file_to_write, delimiter=',', quotechar='\'', quoting=csv.QUOTE_MINIMAL)
        spam_writer.writerow(["Number of nodes", "Number of cycles"])
        spam_writer.writerow([nb_nodes, nb_cycles])
    else:
        csv_file_to_write = open('out/results.csv', 'a')
        spam_writer = csv.writer(csv_file_to_write, delimiter=',', quotechar='\'', quoting=csv.QUOTE_MINIMAL)
        spam_writer.writerow([])

    csv_file_to_write.flush()
    csv_file_to_write.close()
# --------------------------------------


if __name__ == '__main__':

    # setup logger
    logger = logging.getLogger(__file__)
    logger.setLevel(logging.DEBUG)
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    console.setFormatter(formatter)

    logger.addHandler(console)

    if len(sys.argv) < 3:
        logger.error("Invocation: ./echo.py <conf_file> <run_id>")
        sys.exit()

    if not os.path.exists("out/"):
        os.makedirs("out/")
    output = open("output.txt", 'a')

    dumpPath = sys.argv[1]
    confFile = dumpPath + '/conf.yaml'
    runId = int(sys.argv[2])
    f = open(confFile)

    gc.enable()

    create_new = True
    save_network_connections = False
    file_name = ""
    results_name = "results"
    number_of_bad_nodes = 0
    if len(sys.argv) > 3:
        i = 3
        while i < len(sys.argv):
            if sys.argv[i] == "-cn":
                create_new = sys.argv[i+1]
            elif sys.argv[i] == "-sn":
                save_network_connections = sys.argv[i+1]
            elif sys.argv[i] == "-ln":
                create_new = False
                save_network_connections = False
                file_name = sys.argv[i+1]
            elif sys.argv[i] == "-rsn":
                results_name = sys.argv[i+1]
            elif sys.argv[i] == "-bn":
                number_of_bad_nodes = int(sys.argv[i+1])
            else:
                raise ValueError("Input {} is invalid".format(sys.argv[i]))
            i += 2

    if not create_new and file_name == "":
        raise ValueError("Invalid combination of inputs create_new and file_name")

    # load configuration file
    configure(yaml.load(f))
    logger.info('Configuration done')

    # start simulation
    init()
    logger.info('Init done')
    # run the simulation
    sim.run()
    logger.info('Run done')
    # finish simulation, compute stats
    output.close()
    wrapup()
    logger.info("That's all folks!")
