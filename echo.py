# Sample simulator demo
# Miguel Matos - miguel.marques.matos@tecnico.ulisboa.pt
# (c) 2012-2018
from __future__ import division

import csv
import time
from collections import defaultdict
import math
import random
import sys
import os

import yaml
import cPickle
import logging
import numpy
from sim import sim
import utils

LOG_TO_FILE = False
HOP_BASED_BROADCAST = True

BLOCK_ID, BLOCK_PARENT_ID, BLOCK_HEIGHT, BLOCK_TIMESTAMP, BLOCK_GEN_NODE, BLOCK_TX, BLOCK_TTL, BLOCK_EXTRA_TX \
    = 0, 1, 2, 3, 4, 5, 6, 7
TX_ID, TX_CONTENT, TX_GEN_NODE, TX_SIZE = 0, 1, 2, 3
INV_TYPE, INV_CONTENT_ID = 0, 1
HEADER_ID, HEADER_PARENT_ID, HEADER_TIMESTAMP, HEADER_GEN_NODE = 0, 1, 2, 3

CURRENT_CYCLE, NODE_CURRENT_BLOCK, NODE_INV, NODE_RECEIVED_BLOCKS, NODE_PARTIAL_BLOCKS, NODE_MEMPOOL, \
    NODE_BLOCKS_ALREADY_REQUESTED, NODE_TX_ALREADY_REQUESTED, NODE_TIME_TO_GEN, NODE_NEIGHBOURHOOD, NODE_NEIGHBOURHOOD_INV, \
    NODE_NEIGHBOURHOOD_STATS, MSGS = 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12

NODE_INV_RECEIVED_BLOCKS, NODE_INV_RECEIVED_TX = 0, 1

NEIGHBOURHOOD_KNOWN_BLOCKS, NEIGHBOURHOOD_KNOWN_TX, NEIGHBOURHOOD_TX_TO_SEND = 0, 1, 2

TOP_N_NODES, STATS = 0, 1

TOTAL_TLL, TOTAL_MSG_RECEIVED = 0, 1

INV_MSG, GETHEADERS_MSG, HEADERS_MSG, GETDATA_MSG, BLOCK_MSG, CMPCTBLOCK_MSG, GETBLOCKTXN_MSG, BLOCKTXN_MSG, TX_MSG, MISSING_TX \
    = 0, 1, 2, 3, 4, 5, 6, 7, 8, 9

SENT, RECEIVED = 0, 1


def init():
    # schedule execution for all nodes
    for nodeId in nodeState:
        sim.schedulleExecution(CYCLE, nodeId)

    # other things such as periodic measurements can also be scheduled
    # to schedule periodic measurements use the following
    # for c in range(nbCycles * 10):
    #    sim.scheduleExecutionFixed(MEASURE_X, nodeCycle * (c + 1))


def CYCLE(myself):
    global nodeState

    # with churn the node might be gone
    if myself not in nodeState:
        return

    # show progress for one node
    if myself == 0:
        logger.info('cycle {}'.format(nodeState[myself][CURRENT_CYCLE]))

    # If a node can generate transactions
    i = 0
    n = get_nb_of_tx_to_gen(myself, len(nodeState), nodeState[myself][CURRENT_CYCLE])
    while i < n:
        generate_new_tx(myself)
        i = i + 1

    # If the node can generate a block
    if nodeState[myself][NODE_TIME_TO_GEN] == -1:
        next_t_to_gen(myself)

    if nodeState[myself][NODE_TIME_TO_GEN] == nodeState[myself][CURRENT_CYCLE] and \
            (max_block_number == 0 or block_id < max_block_number):
        next_t_to_gen(myself)
        if myself in miners or (myself not in miners and random.random() < 0.052):
            new_block = generate_new_block(myself)

            # Check if can send as cmpct or send through inv
            for target in nodeState[myself][NODE_NEIGHBOURHOOD]:
                if check_availability(myself, target, "block", new_block[BLOCK_PARENT_ID]):
                    sim.send(CMPCTBLOCK, target, myself, "CMPCTBLOCK", cmpctblock(new_block))
                    nodeState[myself][MSGS][CMPCTBLOCK_MSG][SENT] += 1
                    update_neighbourhood_inv(myself, target, "block", new_block[BLOCK_ID])

                else:
                    vInv = [("MSG_BLOCK", new_block[BLOCK_ID])]
                    # TODO change this send header and inv of possible parents
                    sim.send(INV, target, myself, "INV", vInv)
                    nodeState[myself][MSGS][INV_MSG][SENT] += 1

    # Send new transactions either created or received
    broadcast_invs(myself)

    nodeState[myself][CURRENT_CYCLE] += 1
    # schedule next execution
    if nodeState[myself][CURRENT_CYCLE] < nbCycles:
        sim.schedulleExecution(CYCLE, myself)


def INV(myself, source, msg1, vInv):
    global nodeState

    new_connection(myself, source)

    # logger.info("Node {} Received {} from {}".format(myself, msg1, source))
    nodeState[myself][MSGS][INV_MSG][RECEIVED] += 1

    ask_for = []
    headers_to_request = []
    for inv in vInv:
        if inv[INV_TYPE] == "MSG_TX":
            update_neighbourhood_inv(myself, source, "tx", inv[INV_CONTENT_ID])
            seen_tx = have_it(myself, "tx", inv[INV_CONTENT_ID])
            if not seen_tx and inv[INV_CONTENT_ID] not in nodeState[myself][NODE_TX_ALREADY_REQUESTED]:
                ask_for.append(inv)
                nodeState[myself][NODE_TX_ALREADY_REQUESTED].add(inv[INV_CONTENT_ID])

        elif inv[INV_TYPE] == "MSG_BLOCK":
            update_neighbourhood_inv(myself, source, "block", inv[INV_CONTENT_ID])
            seen_block = have_it(myself, "block", inv[INV_CONTENT_ID])
            if not seen_block:
                headers_to_request.append(inv[INV_CONTENT_ID])

        else:
            # logger.info("Node {} Received INV from {} with invalid inv type {}".format(myself, source, inv))
            raise ValueError('INV, else, node received invalid inv type. This condition is not coded')

    if ask_for:
        sim.send(GETDATA, source, myself, "GETDATA", ask_for)
        nodeState[myself][MSGS][GETDATA_MSG][SENT] += 1

    if headers_to_request:
        sim.send(GETHEADERS, source, myself, "GETHEADERS", headers_to_request)
        nodeState[myself][MSGS][GETHEADERS_MSG][SENT] += 1


def GETHEADERS(myself, source, msg1, get_headers):
    global nodeState

    new_connection(myself, source)

    # logger.info("Node {} Received {} from {}".format(myself, msg1, source))
    nodeState[myself][MSGS][GETHEADERS_MSG][RECEIVED] += 1

    headers_to_send = []
    for id in get_headers:
        block = get_block(myself, id)
        if block is not None:
            headers_to_send.append(get_block_header(block))
        else:
            # logger.info("Node {} Received header from {} INVALID ID in header!!!".format(myself, source))
            raise ValueError('GETHEADERS, else, node received invalid headerID')

    sim.send(HEADERS, source, myself, "HEADERS", headers_to_send)
    nodeState[myself][MSGS][HEADERS_MSG][SENT] += 1


def HEADERS(myself, source, msg1, headers):
    global nodeState

    new_connection(myself, source)

    # logger.info("Node {} Received {} from {}".format(myself, msg1, source))
    nodeState[myself][MSGS][HEADERS_MSG][RECEIVED] += 1

    process_new_headers(myself, source, headers)
    # TODO process_new_headers might return without doing anything check if we should also return
    data_to_request = get_data_to_request(myself, source)
    if len(data_to_request) <= 16:
        # If is a new block in the main chain try and direct fetch
        sim.send(GETDATA, source, myself, "GETDATA", data_to_request)
        nodeState[myself][MSGS][GETDATA_MSG][SENT] += 1
    else:
        # Else rely on other means of download
        # TODO Fix this case still don't know how it's done
        # logger.info("Node {} received more than 16 headers from {} INVALID!!!".format(myself, source))
        raise ValueError('HEADERS, else, this condition is not coded')


def GETDATA(myself, source, msg1, requesting_data):
    global nodeState

    new_connection(myself, source)

    # logger.info("Node {} Received {} from {}".format(myself, msg1, source))
    nodeState[myself][MSGS][GETDATA_MSG][RECEIVED] += 1

    for inv in requesting_data:
        if inv[INV_TYPE] == "MSG_TX":
            tx = get_transaction(myself, inv[INV_CONTENT_ID])
            if tx is not None:
                sim.send(TX, source, myself, "TX", tx)
                nodeState[myself][MSGS][TX_MSG][SENT] += 1
                update_neighbourhood_inv(myself, source, "tx", tx[TX_ID])

        elif inv[INV_TYPE] == "MSG_BLOCK":
            block = get_block(myself, inv[INV_CONTENT_ID])
            if block is not None:
                sim.send(BLOCK, source, myself, "BLOCK", block)
                nodeState[myself][MSGS][BLOCK_MSG][SENT] += 1
                update_neighbourhood_inv(myself, source, "block", block[BLOCK_ID])
            else:
                # This shouldn't happen in a simulated scenario
                raise ValueError('GETDATA, MSG_BLOCK else, this condition is not coded and shouldn\'t happen')

        else:
            # This shouldn't happen in a simulated scenario
            raise ValueError('GETDATA, else, this condition is not coded and shouldn\'t happen')


def BLOCK(myself, source, msg1, block):
    global nodeState

    new_connection(myself, source)

    # logger.info("Node {} Received {} from {}".format(myself, msg1, source))
    nodeState[myself][MSGS][BLOCK_MSG][RECEIVED] += 1

    if block[BLOCK_ID] in nodeState[myself][NODE_BLOCKS_ALREADY_REQUESTED]:
        nodeState[myself][NODE_BLOCKS_ALREADY_REQUESTED].remove(block[BLOCK_ID])

    process_block(myself, source, block, "BLOCK")


def CMPCTBLOCK(myself, source, msg1, cmpctblock):
    global nodeState

    new_connection(myself, source)

    # logger.info("Node {} Received {} from {}".format(myself, msg1, source))
    nodeState[myself][MSGS][CMPCTBLOCK_MSG][RECEIVED] += 1

    in_mem_cmpctblock = get_cmpctblock(myself, cmpctblock[BLOCK_ID])
    if have_it(myself, "block", cmpctblock[BLOCK_ID]) or in_mem_cmpctblock is not None:
        update_neighbour_statistics(myself, source, cmpctblock[BLOCK_TTL])
        update_neighbourhood_inv(myself, source, "block", cmpctblock[BLOCK_ID])
        return

    if cmpctblock[BLOCK_EXTRA_TX]:
        for tx in cmpctblock[BLOCK_EXTRA_TX]:
            if tx not in nodeState[myself][NODE_MEMPOOL]:
                nodeState[myself][NODE_MEMPOOL].append(tx)
                nodeState[myself][NODE_TX_ALREADY_REQUESTED].remove(tx[TX_ID])


    # Check if we have all tx
    tx_to_request = []
    i = 0
    while i < len(cmpctblock[BLOCK_TX]):
        tx = get_transaction(myself, cmpctblock[BLOCK_TX][i])
        if tx is None:
            tx_to_request.append(cmpctblock[BLOCK_TX][i])
        else:
            cmpctblock[BLOCK_TX][i] = tx
        i += 1

    if tx_to_request:
        sim.send(GETBLOCKTXN, source, myself, "GETBLOCKTXN", (cmpctblock[BLOCK_ID], tx_to_request))
        nodeState[myself][MSGS][GETBLOCKTXN_MSG][SENT] += 1
        nodeState[myself][MSGS][MISSING_TX] += len(tx_to_request)
        nodeState[myself][NODE_PARTIAL_BLOCKS].append(cmpctblock[:BLOCK_EXTRA_TX])
    else:
        process_block(myself, source, cmpctblock[:BLOCK_EXTRA_TX], "CMPCTBLOCK")


def GETBLOCKTXN(myself, source, msg1, tx_request):
    global nodeState

    new_connection(myself, source)

    # logger.info("Node {} Received {} from {}".format(myself, msg1, source))
    nodeState[myself][MSGS][GETBLOCKTXN_MSG][RECEIVED] += 1

    block = get_block(myself, tx_request[0])
    tx_to_send = []
    for tx in block[BLOCK_TX]:
        if tx[TX_ID] in tx_request[1]:
            tx_to_send.append(tx)

    if len(tx_to_send) != len(tx_request[1]):
        # logger.info("Node {} Received invalid tx_id in GETBLOCKTXN from {} INVALID!!!".format(myself, source))
        raise ValueError('GETBLOCKTXN, if, this condition is not coded invalid size req tx != size sent tx')

    if tx_to_send:
        sim.send(BLOCKTXN, source, myself, "BLOCKTXN", (tx_request[0], tx_to_send))
        nodeState[myself][MSGS][BLOCKTXN_MSG][SENT] += 1
    else:
        raise ValueError('GETBLOCKTXN, else, this condition is not coded empty tx_to_send')


def BLOCKTXN(myself, source, msg1, tx_requested):
    global nodeState

    new_connection(myself, source)

    # logger.info("Node {} Received {} from {}".format(myself, msg1, source))
    nodeState[myself][MSGS][BLOCKTXN_MSG][RECEIVED] += 1

    if have_it(myself, "block", tx_requested[0]):
        return

    for tx in tx_requested[1]:
        if tx[TX_ID] in nodeState[myself][NODE_TX_ALREADY_REQUESTED]:
            nodeState[myself][NODE_TX_ALREADY_REQUESTED].remove(tx[TX_ID])

    process_block(myself, source, build_cmpctblock(myself, tx_requested), "CMPCTBLOCK")


def TX(myself, source, msg1, tx):
    global nodeState

    new_connection(myself, source)

    # logger.info("Node {} Received {} from {}".format(myself, msg1, source))
    nodeState[myself][MSGS][TX_MSG][RECEIVED] += 1

    if tx[TX_ID] in nodeState[myself][NODE_TX_ALREADY_REQUESTED]:
        nodeState[myself][NODE_TX_ALREADY_REQUESTED].remove(tx[TX_ID])

    update_neighbourhood_inv(myself, source, "tx", tx[TX_ID])
    if not have_it(myself, "tx", tx[TX_ID]):
        update_have_it(myself, "tx", tx[TX_ID])
        nodeState[myself][NODE_MEMPOOL].append(tx)
        push_to_send(myself, tx[TX_ID])


def next_t_to_gen(myself):
    global nodeState

    y = numpy.random.normal(0.5, 0.13)
    if y > 1:
        x = - 10 * numpy.log(1-0.99)
    else:
        x = - 10 * numpy.log(1-y)

    for tuple in values:
        if tuple[0] <= x < tuple[1]:
            nodeState[myself][NODE_TIME_TO_GEN] += tuple[2]
            return


def generate_new_block(myself):
    global nodeState, block_id

    # First block or
    # Not first block which means getting highest block to be the parent
    tx_array = get_tx_to_block(myself)
    if nodeState[myself][NODE_CURRENT_BLOCK] is None:
        new_block = (block_id, -1, 0, time.time(), myself, tx_array, 0)
    else:
        highest_block = nodeState[myself][NODE_CURRENT_BLOCK]
        new_block = (block_id, highest_block[BLOCK_ID], highest_block[BLOCK_HEIGHT] + 1, time.time(), myself, tx_array, 0)

    # Store the new block
    nodeState[myself][NODE_RECEIVED_BLOCKS].append(new_block)
    nodeState[myself][NODE_INV][NODE_INV_RECEIVED_BLOCKS].add(new_block[BLOCK_ID])
    nodeState[myself][NODE_CURRENT_BLOCK] = new_block
    block_id += 1
    return new_block


def inc_tll(block):
    lst = list(block)
    lst[BLOCK_TTL] += 1
    return tuple(lst)


def get_block(myself, block_id):
    for item in reversed(nodeState[myself][NODE_RECEIVED_BLOCKS]):
        if item[0] == block_id:
            return item
    return None


def update_neighbour_statistics(myself, source, block_ttl):
    nodeState[myself][NODE_NEIGHBOURHOOD_STATS][STATS][source][TOTAL_TLL] += block_ttl
    nodeState[myself][NODE_NEIGHBOURHOOD_STATS][STATS][source][TOTAL_MSG_RECEIVED] += 1
    total_ttl = nodeState[myself][NODE_NEIGHBOURHOOD_STATS][STATS][source][TOTAL_TLL]
    total_msg = nodeState[myself][NODE_NEIGHBOURHOOD_STATS][STATS][source][TOTAL_MSG_RECEIVED]
    update_top(myself, source, total_ttl/total_msg)


def update_top(myself, source, score):
    if source in nodeState[myself][NODE_NEIGHBOURHOOD_STATS][TOP_N_NODES]:
        return

    if not nodeState[myself][NODE_NEIGHBOURHOOD_STATS][TOP_N_NODES] or \
            len(nodeState[myself][NODE_NEIGHBOURHOOD_STATS][TOP_N_NODES]) < top_nodes_size:
        nodeState[myself][NODE_NEIGHBOURHOOD_STATS][TOP_N_NODES].append(source)

    worst_score = -1
    worst_index = -1
    for i in range(0, len(nodeState[myself][NODE_NEIGHBOURHOOD_STATS][TOP_N_NODES])):
        node = nodeState[myself][NODE_NEIGHBOURHOOD_STATS][TOP_N_NODES][i]
        total_ttl = nodeState[myself][NODE_NEIGHBOURHOOD_STATS][STATS][node][TOTAL_TLL]
        total_msg = nodeState[myself][NODE_NEIGHBOURHOOD_STATS][STATS][node][TOTAL_MSG_RECEIVED]
        member_score = total_ttl/total_msg
        if member_score <= score:
            continue
        elif member_score > score and worst_score < member_score:
            worst_score = member_score
            worst_index = i
        else:
            continue

    nodeState[myself][NODE_NEIGHBOURHOOD_STATS][TOP_N_NODES][worst_index] = source


def process_block(myself, source, block, type):
    global nodeState

    # Check if it's a new block
    if not have_it(myself, "block", block[BLOCK_ID]):
        if myself == 0:
            write_to_log("blocksReceived", str(time.time()) + " " + str(block[BLOCK_ID]) + " " + str(block[BLOCK_TIMESTAMP])
                         + " " + type)
        update_have_it(myself, "block", block[BLOCK_ID])
        update_block(myself, block)
        if nodeState[myself][NODE_CURRENT_BLOCK] is None or \
                block[BLOCK_HEIGHT] > nodeState[myself][NODE_CURRENT_BLOCK][BLOCK_HEIGHT]:
            nodeState[myself][NODE_CURRENT_BLOCK] = block
        next_t_to_gen(myself)

        # Remove tx from MEMPOOL and from vINV_TX_TO_SEND
        update_tx(myself, block)

        # Broadcast new block
        update_neighbour_statistics(myself, source, block[BLOCK_TTL])
        block_to_send = inc_tll(block)
        update_neighbourhood_inv(myself, source, "block", block_to_send[BLOCK_ID])
        for target in nodeState[myself][NODE_NEIGHBOURHOOD]:
            if target == source or check_availability(myself, target, "block", block_to_send[BLOCK_ID]):
                continue
            elif check_availability(myself, target, "block", block_to_send[BLOCK_PARENT_ID]):
                sim.send(CMPCTBLOCK, target, myself, "CMPCTBLOCK", cmpctblock(block_to_send))
                nodeState[myself][MSGS][CMPCTBLOCK_MSG][SENT] += 1
                update_neighbourhood_inv(myself, target, "block", block_to_send[BLOCK_ID])
            else:
                sim.send(HEADERS, target, myself, "HEADERS", [get_block_header(block_to_send)])
                nodeState[myself][MSGS][HEADERS_MSG][SENT] += 1

    else:
        update_neighbour_statistics(myself, source, block[BLOCK_TTL])
        update_neighbourhood_inv(myself, source, "block", block[BLOCK_ID])


def update_tx(myself, block):
    global nodeState

    for tx in block[BLOCK_TX]:
        if tx in nodeState[myself][NODE_MEMPOOL]:
            nodeState[myself][NODE_MEMPOOL].remove(tx)

        for neighbour in nodeState[myself][NODE_NEIGHBOURHOOD]:
            update_neighbourhood_inv(myself, neighbour, "tx", tx[TX_ID])

        if tx[TX_ID] in nodeState[myself][NODE_TX_ALREADY_REQUESTED]:
            nodeState[myself][NODE_TX_ALREADY_REQUESTED].remove(tx[TX_ID])


def update_block(myself, block):
    global nodeState

    if not nodeState[myself][NODE_RECEIVED_BLOCKS]:
        nodeState[myself][NODE_RECEIVED_BLOCKS].append(block)
        return

    i = len(nodeState[myself][NODE_RECEIVED_BLOCKS]) - 1
    while i >= 0:
        if nodeState[myself][NODE_RECEIVED_BLOCKS][i][0] == block[0] and \
                len(nodeState[myself][NODE_RECEIVED_BLOCKS][i]) < block:
            nodeState[myself][NODE_RECEIVED_BLOCKS][i] = block
            return
        i = i - 1

    nodeState[myself][NODE_RECEIVED_BLOCKS].append(block)


def cmpctblock(block):
    cmpct_tx = []
    for tx in block[BLOCK_TX]:
        cmpct_tx.append(tx[TX_ID])
    return block[BLOCK_ID], block[BLOCK_PARENT_ID], block[BLOCK_HEIGHT], block[BLOCK_TIMESTAMP], block[BLOCK_GEN_NODE], cmpct_tx,\
           block[BLOCK_TTL], get_extra_tx_to_send(block[BLOCK_TX])


def get_cmpctblock(myself, block_id):
    for partial_block in nodeState[myself][NODE_PARTIAL_BLOCKS]:
        if partial_block[BLOCK_ID] == block_id:
            return partial_block

    return None


def build_cmpctblock(myself, block_and_tx):
    cmpctblock = get_cmpctblock(myself, block_and_tx[0])

    if cmpctblock is None:
        raise ValueError('build_cmpctblock,  if cmpctblock is None, this condition is not coded, '
                         'cmpctblock_id not in partialblocks')

    i = 0
    while i < len(cmpctblock[BLOCK_TX]):
        if isinstance(cmpctblock[BLOCK_TX][i], int):
            for tx_in_block in block_and_tx[1]:
                if cmpctblock[BLOCK_TX][i] == tx_in_block[TX_ID]:
                    if myself == 0:
                        write_to_log("missingTxs", str(time.time()) + " " + str(cmpctblock[BLOCK_ID]) + " "
                                     + str(tx_in_block[TX_ID]) + " " + str(i) + "/" + str(len(cmpctblock[BLOCK_TX])))
                    cmpctblock[BLOCK_TX][i] = tx_in_block
                    block_and_tx[1].remove(tx_in_block)
                    break
            i += 1
        elif len(cmpctblock[BLOCK_TX][i]) == 4:
            i += 1
        else:
            raise ValueError('build_cmpctblock, else, this condition is not coded, tx in block but not in mempool')

    return cmpctblock


def get_extra_tx_to_send(tx_array):
    return []


def get_block_header(block):
    return block[BLOCK_ID], block[BLOCK_PARENT_ID], block[BLOCK_TIMESTAMP], block[BLOCK_GEN_NODE]


def have_it(myself, type, id):
    global nodeState

    if type != "block" and type != "tx":
        print("check_availability strange type {}".format(type))
        exit(-1)

    if (type == "block" and id in nodeState[myself][NODE_INV][NODE_INV_RECEIVED_BLOCKS]) or \
            (type == "tx" and id in nodeState[myself][NODE_INV][NODE_INV_RECEIVED_TX]):
        return True
    return False


def update_have_it(myself, type, id):
    global nodeState

    if type != "block" and type != "tx":
        print("check_availability strange type {}".format(type))
        exit(-1)

    if type == "block":
        nodeState[myself][NODE_INV][NODE_INV_RECEIVED_BLOCKS].add(id)
    elif type == "tx":
        nodeState[myself][NODE_INV][NODE_INV_RECEIVED_TX].add(id)
    else:
        print("update_inv else condition reached with type: {} and id: {}".format(type, id))
        exit(-1)


# Neighbourhood update and check functions
def update_neighbourhood_inv(myself, target, type, id):
    global nodeState

    if type != "block" and type != "tx":
        print("check_availability strange type {}".format(type))
        exit(-1)

    if type == "block":
        if id == -1:
            return
        nodeState[myself][NODE_NEIGHBOURHOOD_INV][target][NEIGHBOURHOOD_KNOWN_BLOCKS].add(id)
    elif type == "tx":
        nodeState[myself][NODE_NEIGHBOURHOOD_INV][target][NEIGHBOURHOOD_KNOWN_TX].add(id)
        if id in nodeState[myself][NODE_NEIGHBOURHOOD_INV][target][NEIGHBOURHOOD_TX_TO_SEND]:
            nodeState[myself][NODE_NEIGHBOURHOOD_INV][target][NEIGHBOURHOOD_TX_TO_SEND].remove(id)
    else:
        print("update_inv else condition reached with type: {} and id: {}".format(type, id))
        exit(-1)


def check_availability(myself, target, type, id):
    if type != "block" and type != "tx":
        print("check_availability strange type {}".format(type))
        exit(-1)

    if (type == "block" and id in nodeState[myself][NODE_NEIGHBOURHOOD_INV][target][NEIGHBOURHOOD_KNOWN_BLOCKS]) or \
            (type == "tx" and id in nodeState[myself][NODE_NEIGHBOURHOOD_INV][target][NEIGHBOURHOOD_KNOWN_TX]):
        return True
    return False


def push_to_send(myself, id):
    global nodeState

    for node in nodeState[myself][NODE_NEIGHBOURHOOD]:
        if not check_availability(myself, node, "tx", id):
            nodeState[myself][NODE_NEIGHBOURHOOD_INV][node][NEIGHBOURHOOD_TX_TO_SEND].add(id)
# -----------------------


def generate_new_tx(myself):
    global nodeState, tx_id

    new_tx = (tx_id, "This transaction spends " + str(random.randint(0, 100)) + " Bitcoins", myself, 700)
    nodeState[myself][NODE_INV][NODE_INV_RECEIVED_TX].add(new_tx[TX_ID])
    nodeState[myself][NODE_MEMPOOL].append(new_tx)
    push_to_send(myself, new_tx[TX_ID])

    tx_id += 1


def get_transaction(myself, tx_id):
    for tx in reversed(nodeState[myself][NODE_MEMPOOL]):
        if tx[TX_ID] == tx_id:
            return tx
    return None


def get_tx_in_block(block, tx_id):
    for tx in block[BLOCK_TX]:
        if tx[TX_ID] == tx_id:
            return tx
    return None


def get_nb_of_tx_to_gen(myself, size, cycle):
    n = number_of_tx_to_gen_per_cycle//size

    if n != 0:
        tx_gened[cycle] += n
        return n
    else:
        if myself in nodes_to_gen_tx[cycle]:
            tx_gened[cycle] += 1
            return 1
        return 0


def get_tx_to_block(myself):
    global nodeState

    size = 0
    tx_array = []
    list_to_iter = list(nodeState[myself][NODE_MEMPOOL])
    for tx in list_to_iter:
        if size + tx[TX_SIZE] <= max_block_size:
            size += tx[TX_SIZE]
            tx_array.append(tx)
            nodeState[myself][NODE_MEMPOOL].remove(tx)
        elif size + min_tx_size > max_block_size:
            break
        else:
            continue
    return tx_array


def broadcast_invs(myself):
    global nodeState


    nodes_to_send = get_nodes_to_send(myself)


    for target in nodes_to_send:
        if len(nodeState[myself][NODE_NEIGHBOURHOOD_INV][target][NEIGHBOURHOOD_TX_TO_SEND]) > 0:
            inv_to_send = []
            for tx in nodeState[myself][NODE_NEIGHBOURHOOD_INV][target][NEIGHBOURHOOD_TX_TO_SEND]:
                if not check_availability(myself, target, "tx", tx):
                    inv_to_send.append(("MSG_TX", tx))
            sim.send(INV, target, myself, "INV", inv_to_send)
            nodeState[myself][MSGS][INV_MSG][SENT] += 1
            nodeState[myself][NODE_NEIGHBOURHOOD_INV][target][NEIGHBOURHOOD_TX_TO_SEND].clear()


def get_nodes_to_send(myself):
    if not HOP_BASED_BROADCAST or not nodeState[myself][NODE_NEIGHBOURHOOD_STATS][TOP_N_NODES]:
        return nodeState[myself][NODE_NEIGHBOURHOOD]

    total = top_nodes_size * 2
    top_nodes = nodeState[myself][NODE_NEIGHBOURHOOD_STATS][TOP_N_NODES]
    if len(nodeState[myself][NODE_NEIGHBOURHOOD]) < total:
        total = len(nodeState[myself][NODE_NEIGHBOURHOOD]) - len(top_nodes)
    else:
        total = total - len(top_nodes)

    collection_of_neighbours = list(nodeState[myself][NODE_NEIGHBOURHOOD])
    for node in top_nodes:
        if node in collection_of_neighbours:
            collection_of_neighbours.remove(node)

    random_nodes = random.sample(collection_of_neighbours, total)

    return top_nodes + random_nodes


def process_new_headers(myself, source, headers):
    global nodeState

    for header in headers:
        block = get_block(myself, header[HEADER_ID])
        parent_block = get_block(myself, header[HEADER_PARENT_ID])
        update_neighbourhood_inv(myself, source, "block", header[HEADER_ID])
        update_neighbourhood_inv(myself, source, "block", header[HEADER_PARENT_ID])

        if parent_block is None and header[HEADER_PARENT_ID] != -1:
            # TODO REFACTOR
            logger.info("Node {} Received a header with a parent that doesn't connect id={} THIS NEEDS TO BE CODED!!"
                        .format(myself, header[HEADER_PARENT_ID]))
            headers_to_request = [header[HEADER_PARENT_ID], header[HEADER_ID]]
            sim.send(GETHEADERS, source, myself, "GETHEADERS", headers_to_request)
            nodeState[myself][MSGS][GETHEADERS_MSG][SENT] += 1
            continue

        elif (parent_block is not None or header[HEADER_PARENT_ID] == -1) and block is None:
            nodeState[myself][NODE_RECEIVED_BLOCKS].append(header)


def get_data_to_request(myself, source):
    global nodeState

    data_to_request = []
    for block in reversed(nodeState[myself][NODE_RECEIVED_BLOCKS]):
        if len(block) == 4 and check_availability(myself, source, "block", block[BLOCK_ID]) and \
                block[BLOCK_ID] not in nodeState[myself][NODE_BLOCKS_ALREADY_REQUESTED]:
            data_to_request.append(("MSG_BLOCK", block[BLOCK_ID]))
            nodeState[myself][NODE_BLOCKS_ALREADY_REQUESTED].add(block[BLOCK_ID])
        elif len(block) == 4 or len(block) == 7:
            continue
        else:
            # This condition shouldn't happen in a simulated scenario
            raise ValueError("get_data_to_request, else, there are tuples in the NODE_RECEIVED_BLOCKS that do not have an "
                             "expected size. This condition is not coded and shouldn't happen {}".format(block))

    return data_to_request


def new_connection(myself, source):
    global nodeState

    if len(nodeState[myself][NODE_NEIGHBOURHOOD]) > 125:
        raise ValueError("Number of connections in one node exceed the maximum allowed")

    if source in nodeState[myself][NODE_NEIGHBOURHOOD]:
        return
    else:
        nodeState[myself][NODE_NEIGHBOURHOOD].append(source)
        nodeState[myself][NODE_NEIGHBOURHOOD_INV][source] = [set(), set(), set()]
        nodeState[myself][NODE_NEIGHBOURHOOD_STATS][STATS][source] = [0, 0]


def write_to_log(file, line):
    f = open("logs/" + file + ".txt", "a+")
    f.write(line + "\n")
    f.close()


def wrapup():
    global nodeState
    logger.info("Wrapping up")
    #logger.info(nodeState)

    inv_messages = map(lambda x: nodeState[x][MSGS][INV_MSG], nodeState)
    getheaders_messages = map(lambda x: nodeState[x][MSGS][GETHEADERS_MSG], nodeState)
    headers_messages = map(lambda x: nodeState[x][MSGS][HEADERS_MSG], nodeState)
    getdata_messages = map(lambda x: nodeState[x][MSGS][GETDATA_MSG], nodeState)
    block_messages = map(lambda x: nodeState[x][MSGS][BLOCK_MSG], nodeState)
    cmpctblock_messages = map(lambda x: nodeState[x][MSGS][CMPCTBLOCK_MSG], nodeState)
    getblocktx_messages = map(lambda x: nodeState[x][MSGS][GETBLOCKTXN_MSG], nodeState)
    blocktx_messages = map(lambda x: nodeState[x][MSGS][BLOCKTXN_MSG], nodeState)
    tx_messages = map(lambda x: nodeState[x][MSGS][TX_MSG], nodeState)
    missing_tx = map(lambda x: nodeState[x][MSGS][MISSING_TX], nodeState)

    sum_received_blocks = map(lambda x: nodeState[x][NODE_RECEIVED_BLOCKS], nodeState)
    receivedBlocks = map(lambda x: map(lambda y: (sum_received_blocks[x][y][0], sum_received_blocks[x][y][1],
                                                  sum_received_blocks[x][y][2], sum_received_blocks[x][y][3],
                                                  sum_received_blocks[x][y][4], sum_received_blocks[x][y][6]),
                                       xrange(len(sum_received_blocks[x]))), nodeState)
    sum_received_blocks = map(lambda x: map(lambda y: sum_received_blocks[x][y][0], xrange(len(sum_received_blocks[x]))), nodeState)

    # dump data into gnuplot format
    utils.dump_as_gnu_plot([inv_messages, getheaders_messages, headers_messages, getdata_messages, block_messages,
                         cmpctblock_messages, getblocktx_messages, blocktx_messages, tx_messages, sum_received_blocks,
                         receivedBlocks],
                        dumpPath + '/messages-' + str(runId) + '.gpData',
                        ['inv getheaders headers getdata block cmpctblock getblocktx blocktx tx'
                         '           sum_received_blocks                    receivedBlocks'])
 #   with open(dumpPath + '/dumps-' + str(runId) + '.obj', 'w') as f:
  #      cPickle.dump(receivedMessages, f)
   #     cPickle.dump(sentMessages, f)
    sum_inv = 0
    sum_getData = 0
    sum_tx = 0
    sum_getBlockTX = 0
    sum_missingTX = 0
    for i in range(0, nbNodes):
        sum_inv += inv_messages[i][SENT]
        sum_getData += getdata_messages[i][SENT]
        sum_tx += tx_messages[i][SENT]
        sum_getBlockTX += getblocktx_messages[i][SENT]
        sum_missingTX += missing_tx[i]

    if not os.path.isfile('out/results.csv'):
        with open('out/results.csv', 'w') as csv_file_to_write:
            spam_writer = csv.writer(csv_file_to_write, delimiter=',', quotechar='\'', quoting=csv.QUOTE_MINIMAL)
            spam_writer.writerow(["Number of nodes", "Number of cycles", "Number of miners", "Extra miners"])
            spam_writer.writerow([nbNodes, nbCycles, number_of_miners, extra_replicas])
            spam_writer.writerow(["Top nodes size", "Avg inv", "Avg getData", "Avg Tx", "Avg getBlockTX", "Avg missing tx"])
            if HOP_BASED_BROADCAST:
                spam_writer.writerow([top_nodes_size, sum_inv/nbNodes, sum_getData/nbNodes, sum_tx/nbNodes, sum_getBlockTX/nbNodes,
                                  sum_missingTX/nbNodes])
            else:
                spam_writer.writerow(
                    ["False", sum_inv / nbNodes, sum_getData / nbNodes, sum_tx / nbNodes, sum_getBlockTX / nbNodes,
                     sum_missingTX / nbNodes])

    else:
        with open('out/results.csv', 'a') as csv_file_to_write:
            spam_writer = csv.writer(csv_file_to_write, delimiter=',', quotechar='\'', quoting=csv.QUOTE_MINIMAL)
            if HOP_BASED_BROADCAST:
                spam_writer.writerow([top_nodes_size, sum_inv/nbNodes, sum_getData/nbNodes, sum_tx/nbNodes, sum_getBlockTX/nbNodes,
                                  sum_missingTX/nbNodes])
            else:
                spam_writer.writerow(
                    ["False", sum_inv / nbNodes, sum_getData / nbNodes, sum_tx / nbNodes, sum_getBlockTX / nbNodes,
                     sum_missingTX / nbNodes])


def createNode(neighbourhood):
    current_cycle = 0
    node_current_block = None
    node_inv = [set(), set()]
    node_received_blocks = []
    node_partial_blocks = []
    node_mempool = []
    node_blocks_already_requested = set()
    node_tx_already_requested = set()
    node_time_to_gen = -1
    node_neighbourhood_inv = {}
    stats = {}
    topx = []
    for neighbour in neighbourhood:
        node_neighbourhood_inv[neighbour] = [set(), set(), set()]
        stats[neighbour] = [0, 0]
    node_neighbourhood_stats = [topx, stats]

    msgs = [[0, 0], [0, 0], [0, 0], [0, 0], [0, 0], [0, 0], [0, 0], [0, 0], [0, 0], 0]

    return [current_cycle, node_current_block, node_inv, node_received_blocks, node_partial_blocks, node_mempool,
            node_blocks_already_requested, node_tx_already_requested, node_time_to_gen, neighbourhood,
            node_neighbourhood_inv, node_neighbourhood_stats, msgs]


def configure(config):
    global nbNodes, nbCycles, prob_generating_block, nodeState, nodeCycle, block_id, max_block_number, tx_id, \
        number_of_tx_to_gen_per_cycle, tx_gened, max_block_size, min_tx_size, max_tx_size, values, nodes_to_gen_tx, miners, \
        top_nodes_size, number_of_miners, extra_replicas

    IS_CHURN = config.get('CHURN', False)
    if IS_CHURN:
        CHURN_RATE = config.get('CHURN_RATE', 0.)
    MESSAGE_LOSS = float(config.get('MESSASE_LOSS', 0))
    if MESSAGE_LOSS > 0:
        sim.setMessageLoss(MESSAGE_LOSS)

    if console_nb_of_nodes == 0:
        nbNodes = config['nbNodes']
    else:
        nbNodes = console_nb_of_nodes

    nbCycles = config['nbCycles']
    nodeCycle = int(config['NODE_CYCLE'])
    neighbourhood_size = int(config['NEIGHBOURHOOD_SIZE'])
    prob_generating_block = config['PROB_GEN_BLOCK']
    max_block_number = int(config['MAX_NUMBER_OF_BLOCKS'])
    number_of_tx_to_gen_per_cycle = config['NUMB_TX_PER_CYCLE']
    nodeDrift = int(nodeCycle * float(config['NODE_DRIFT']))

    max_block_size = int(config['MAX_BLOCK_SIZE'])
    min_tx_size = int(config['MIN_TX_SIZE'])
    max_tx_size = int(config['MAX_TX_SIZE'])
    number_of_miners = int(config['NUMBER_OF_MINERS'])
    extra_replicas = int(config['EXTRA_REPLICAS'])
    top_nodes_size = int(config['TOP_NODES_SIZE'])

    latencyTablePath = config['LATENCY_TABLE']
    latencyValue = None

    f = open("logs/blocksReceived.txt", "w")
    f.close()
    f = open("logs/missingTxs.txt", "w")
    f.close()

    tx_gened = [0] * nbCycles

    values = []
    i = -1
    j = 20
    while i < 20:
        values.append((i, i + 1, j))
        i += 1
        j -= 1

    try:
        with open(latencyTablePath, 'r') as f:
            latencyTable = cPickle.load(f)
    except:
        latencyTable = None
        latencyValue = int(latencyTablePath)
        logger.warn('Using constant latency value: {}'.format(latencyValue))

    latencyTable = utils.check_latency_nodes(latencyTable, nbNodes, latencyValue)
    latencyDrift = eval(config['LATENCY_DRIFT'])

    block_id = 0
    tx_id = 0
    nodeState = defaultdict()
    for n in xrange(nbNodes):
        neighbourhood = random.sample(xrange(nbNodes), neighbourhood_size)
        while neighbourhood.__contains__(n):
            neighbourhood = random.sample(xrange(nbNodes), neighbourhood_size)
        nodeState[n] = createNode(neighbourhood)

    miners = random.sample(xrange(nbNodes), number_of_miners)

    if extra_replicas > 0:
        i = 0
        miners_to_add = []
        for n in xrange(nbNodes, nbNodes + (extra_replicas * number_of_miners)):
            neighbourhood = random.sample(xrange(nbNodes), neighbourhood_size)
            while neighbourhood.__contains__(n) or neighbourhood.__contains__(miners[i]):
                neighbourhood = random.sample(xrange(nbNodes), neighbourhood_size)
            neighbourhood.append(miners[i])
            nodeState[n] = createNode(neighbourhood)
            miners_to_add.append(n)
            i += 1
        miners.append(miners_to_add)

        nbNodes = nbNodes + (extra_replicas * number_of_miners)

    if number_of_tx_to_gen_per_cycle//nbNodes == 0:
        nodes_to_gen_tx = []
        for i in range(0, nbCycles):
            nodes_to_gen_tx.append(random.sample(xrange(nbNodes), number_of_tx_to_gen_per_cycle))

    sim.init(nodeCycle, nodeDrift, latencyTable, latencyDrift)


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

    if LOG_TO_FILE:
        if not os.path.exists("logs/"):
            os.makedirs("logs/")
            # logging.basicConfig(format='%(asctime)s %(message)s', level=logging.DEBUG, filename='logs/echo.log', filemode='w')
    dumpPath = sys.argv[1]
    confFile = dumpPath + '/conf.yaml'
    runId = int(sys.argv[2])
    f = open(confFile)
    console_nb_of_nodes = 0
    if len(sys.argv) == 4:
        console_nb_of_nodes = int(sys.argv[3])

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
    wrapup()
    logger.info("That's all folks!")
