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
from sim import sim
import utils

# Messages structures

# Node structure
# NODES_TO_RELAY - cheat to avoid using a more specialized function
# TODO update total_nodes
CURRENT_CYCLE, MY_IP, KEY, RANDOM, NODE_NEIGHBOURHOOD, NODES_CONNECTED, TRIED_NEW, NODES_TO_RELAY, TRIED_COLLISIONS, MSGS \
    = 0, 1, 2, 3, 4, 5, 6, 7, 8, 9

TRIED, NEW = 0, 1

# Node neighbour structure
F_INBOUND, F_IN_TRIED, F_SHOULD_BAN, IP, TIME, PING_STRC, ADDR_STRC, MISBEHAVIOR, REF_COUNT, LAST_TRY, ATTEMPTS, LAST_SUCCESS\
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


def CYCLE(myself):
    global nodeState

    # with churn the node might be gone
    if myself not in nodeState:
        return

    # show progress for one node
    if myself == 0 and nodeState[myself][CURRENT_CYCLE] % 600 == 0:
        improve_performance(nodeState[myself][CURRENT_CYCLE])
        value = datetime.datetime.fromtimestamp(time.time())
        #output.write('{} id: {} cycle: {}\n'.format(value.strftime('%Y-%m-%d %H:%M:%S'), runId, nodeState[myself][CURRENT_CYCLE]))
        #output.flush()
        print('{} id: {} cycle: {}'.format(value.strftime('%Y-%m-%d %H:%M:%S'), runId, nodeState[myself][CURRENT_CYCLE]))

    # Cheat to avoid using a deterministic function to know which nodes will we send addr's
    if nodeState[myself][CURRENT_CYCLE] % (24 * 60 * 60) == 0:
        update_relays(myself)

    # Make nodes send the version message to their neighbours when they come online
    # TODO make this work with nodes coming online at different times
    for node in nodeState[myself][NODES_CONNECTED]:
        create_neighbour_structures(myself, node, False)
        sim.send(VERSION, node, myself)

    # Main cycle to dispatch messages on nodes which we are connected to
    for node in nodeState[myself][NODES_CONNECTED]:
        cnode = nodeState[myself][NODE_NEIGHBOURHOOD][node]
        send_messages(myself, node, cnode)

    # TODO implement feeler connections

    nodeState[myself][CURRENT_CYCLE] += 1
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

    create_neighbour_structures(myself, source, False)

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

        mark_address_as_good(myself, source, True, nodeState[myself][CURRENT_CYCLE])

    # TODO implement feeler

    return True


def VERACK(myself, source):
    global nodeState

    # TODO prevent nodes from starting communications with this message in case that could happen

    if should_log(myself):
        nodeState[myself][MSGS][VERACK_MSG] += 1

    pto = nodeState[myself][NODE_NEIGHBOURHOOD][source]
    if not pto[F_INBOUND]:
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

    now = nodeState[myself][CURRENT_CYCLE]
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
    ping_time_end = nodeState[myself][CURRENT_CYCLE]
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

        u_bucket = get_new_bucket(myself, nodeState[myself][KEY], source)
        u_bucket_pos = get_bucket_position(nodeState[myself][KEY], True, u_bucket, source)
        clear_new(myself, u_bucket, u_bucket_pos)


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
        return [myself, nodeState[myself][CURRENT_CYCLE]]
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


def add_new_addr(myself, source, addr, time_penalty):
    f_new = False
    now = nodeState[myself][CURRENT_CYCLE]
    pto = None
    if addr[ADDR_ID] in nodeState[myself][NODE_NEIGHBOURHOOD]:
        pto = nodeState[myself][NODE_NEIGHBOURHOOD][addr[ADDR_ID]]

    if addr[ADDR_ID] == source:
        time_penalty = 0

    if pto is not None:
        f_currently_online = (now - addr[TIME] < 24 * 60 * 60)
        update_interval = 60 * 60 if f_currently_online else 24 * 60 * 60
        if addr[TIME] and (not pto[TIME] or pto[TIME] < addr[TIME] - update_interval - time_penalty):
            pto[TIME] = max(0, addr[TIME] - time_penalty)

        if not addr[TIME] or (pto[TIME] and addr[TIME] <= pto[TIME]):
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
        if n_factor > 1 and random.randint(n_factor) != 0:
            return False
    else:
        create_neighbour_structures(myself, addr, None)
        pto = nodeState[myself][NODE_NEIGHBOURHOOD][addr[ADDR_ID]]
        pto[TIME] = max(0, pto[TIME] - time_penalty)
        f_new = True

    u_bucket = get_new_bucket(myself, nodeState[myself][KEY], addr, source)
    u_bucket_pos = get_bucket_position(nodeState[myself][KEY], True, u_bucket, addr)
    vv_new = nodeState[myself][TRIED_NEW][NEW]
    if vv_new[u_bucket][u_bucket_pos] != addr[ADDR_ID]:
        insert = vv_new[u_bucket][u_bucket_pos] == -1
        if not insert:
            cnode = nodeState[myself][NODE_NEIGHBOURHOOD][vv_new[u_bucket][u_bucket_pos]]
            if is_terrible(myself, vv_new[u_bucket][u_bucket_pos]) or cnode[REF_COUNT] > 1 and pto[REF_COUNT] == 0:
                insert = True

        if insert:
            clear_new(myself, vv_new, u_bucket, u_bucket_pos)
            pto[REF_COUNT] += 1
            vv_new[u_bucket][u_bucket_pos] = addr[ADDR_ID]
        else:
            if pto[REF_COUNT] == 0:
                raise ValueError("IMPLEMENT: I should delete the ID")

    return f_new


def get_new_bucket(myself, key, addr, source):
    group = get_group(nodeState[myself][NODE_NEIGHBOURHOOD][source][IP])
    to_hash = str(key) + get_group(addr[IP]) + group
    hash1 = (hashlib.sha256(to_hash)).hexdigest()
    to_hash = str(key) + group + str(hash1 % ADDRMAN_NEW_BUCKETS_PER_SOURCE_GROUP)
    hash2 = (hashlib.sha256(to_hash)).hexdigest()
    return hash2 % ADDRMAN_NEW_BUCKET_COUNT


def get_tried_bucket(key, addr):
    to_hash = str(key) + addr[ADDR_ID]
    hash1 = (hashlib.sha256(to_hash)).hexdigest()
    to_hash = str(key) + get_group(addr[IP]) + str(hash1 % ADDRMAN_TRIED_BUCKETS_PER_GROUP)
    hash2 = (hashlib.sha256(to_hash)).hexdigest()
    return hash2 % ADDRMAN_TRIED_BUCKET_COUNT


def get_bucket_position(key, new, bucket, addr):
    to_hash = str(key) + ('N' if new else 'K') + str(bucket) + addr[ADDR_ID]
    hash1 = (hashlib.sha256(to_hash)).hexdigest()
    return hash1 % ADDRMAN_BUCKET_SIZE


def clear_new(myself, u_bucket, u_bucket_pos):
    new = nodeState[myself][TRIED_NEW][NEW]
    if new[u_bucket][u_bucket_pos] != -1:
        id_delete = new[u_bucket][u_bucket_pos]
        pto = nodeState[myself][NODE_NEIGHBOURHOOD][id_delete]
        pto[REF_COUNT] -= 1
        new[u_bucket][u_bucket_pos] = -1
        # TODO delete if ...
        # if pto[REF_COUNT] == 0:

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
    now = nodeState[myself][CURRENT_CYCLE]
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
    if pto[PING_STRC][PING_NONCE_SENT] == 0 and pto[PING_STRC][PING_TIME_START] + PING_INTERVAL < nodeState[myself][CURRENT_CYCLE]:
        ping_send = True
    if ping_send:
        pto[PING_STRC][PING_TIME_START] = nodeState[myself][CURRENT_CYCLE]
        pto[PING_STRC][PING_NONCE_SENT] = ping_nonce
        ping_nonce += 1
        sim.send(PING, target, myself, pto[PING_STRC][PING_NONCE_SENT])


def send_reject_and_check_if_banned(myself, target, pto):
    # TODO implement
    return


def address_refresh_broadcast(myself, pto):
    if pto[ADDR_STRC][NEXT_LOCAL_ADDR_SEND] < nodeState[myself][CURRENT_CYCLE]:
        push_address(pto, [myself, nodeState[myself][MY_IP], nodeState[myself][CURRENT_CYCLE]])
        pto[ADDR_STRC][NEXT_LOCAL_ADDR_SEND] = poisson_next_send(nodeState[myself][CURRENT_CYCLE], AVG_LOCAL_ADDRESS_BROADCAST_INTERVAL)


def send_addr(myself, target, pto):
    if pto[ADDR_STRC][NEXT_ADDR_SEND] < nodeState[myself][CURRENT_CYCLE]:
        pto[ADDR_STRC][NEXT_ADDR_SEND] = poisson_next_send(nodeState[myself][CURRENT_CYCLE], AVG_ADDRESS_BROADCAST_INTERVAL)
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
def create_network(create_new_network, save_network_conn, neighbourhood_size, filename=""):

    first_time = not os.path.exists("networks/")
    network_first_time = not os.path.exists("networks/" + filename)

    if first_time:
        os.makedirs("networks/")

    if network_first_time or create_new_network:
        create_nodes(neighbourhood_size)
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
            nodeState[n] = create_node(ast.literal_eval(file_to_read.readline()))


def create_node(neighbourhood):
    # TODO give different ip's
    ip = ''
    current_cycle = 0
    key = random.randint()
    msgs = [0] * 6
    neighbourhood_dic = defaultdict()
    bucket = [-1] * 64
    vv_tried = [bucket] * 256
    vv_new = [bucket] * 1024
    tried_new = [vv_tried, vv_new]
    for neighbour in neighbourhood:
        neighbourhood_dic[neighbour] = create_neighbour(False)

    return [current_cycle, ip, key, list(neighbourhood), neighbourhood_dic, neighbourhood, tried_new, defaultdict(), msgs]


def create_neighbour(inbound):
    in_tried = False
    should_ban = False
    ip = ''
    time = 0
    ping = [0, 0, 0, 0]
    addr = [False, 0, 0, [], []]
    misbehaviour = 0
    ref_count = 0
    last_try = 0
    attempts = 0
    last_success = 0
    return [inbound, in_tried, should_ban, ip, time, ping, addr, misbehaviour, ref_count, last_try, attempts, last_success]


def create_neighbour_structures(myself, addr, inbound):
    global nodeState

    # No point in doing this if the node already exists
    if addr[ADDR_ID] in nodeState[myself][NODE_NEIGHBOURHOOD]:
        return

    if len(nodeState[myself][NODES_CONNECTED]) == 125:
        raise ValueError("Number of connections in one node exceed the maximum allowed")

    nodeState[myself][NODE_NEIGHBOURHOOD][addr[ADDR_ID]] = create_neighbour(inbound)


def create_nodes(neighbourhood_size):
    global nodeState

    nodeState = defaultdict()
    for n in xrange(nb_nodes):
        neighbourhood = random.sample(xrange(nb_nodes), neighbourhood_size)
        while neighbourhood.__contains__(n):
            neighbourhood = random.sample(xrange(nb_nodes), neighbourhood_size)
        nodeState[n] = create_node(neighbourhood)


def create_bad_node():
    global bad_nodes

    bad_nodes = random.sample(xrange(nb_nodes), int((number_of_bad_nodes/100) * nb_nodes))


def configure(config):
    global nb_nodes, nb_cycles, nodeState, node_cycle, expert_log, bad_nodes, number_of_bad_nodes, ping_nonce

    node_cycle = int(config['NODE_CYCLE'])

    nb_nodes = config['NUMBER_OF_NODES']
    neighbourhood_size = int(config['NEIGHBOURHOOD_SIZE'])
    if number_of_bad_nodes == 0:
        number_of_bad_nodes = int(config['NUMBER_OF_BAD_NODES'])

    nb_cycles = config['NUMBER_OF_CYCLES']

    expert_log = bool(config['EXPERT_LOG'])
    if expert_log and nb_cycles < INTERVAL:
        raise ValueError("You have to complete more than {} cycles if you have expert_log on".format(INTERVAL))

    create_network(create_new, save_network_connections, neighbourhood_size, file_name)

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


    ping_nonce = 0
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
