# Sample simulator demo
# Miguel Matos - miguel.marques.matos@tecnico.ulisboa.pt
# (c) 2012-2018
from __future__ import division

import ast
import csv
import gc
from copy import copy

import datetime
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
from sortedList import SortedCollection

BLOCK_ID, BLOCK_PARENT_ID, BLOCK_HEIGHT, BLOCK_TIMESTAMP, BLOCK_GEN_NODE, BLOCK_TX, BLOCK_TTL, BLOCK_RECEIVED_TS \
    = 0, 1, 2, 3, 4, 5, 6, 7

INV_TYPE, INV_CONTENT_ID = 0, 1
HEADER_ID, HEADER_PARENT_ID = 0, 1

CURRENT_CYCLE, NODE_CURRENT_BLOCK, NODE_INV, NODE_PARTIAL_BLOCKS, NODE_MEMPOOL, \
    NODE_BLOCKS_ALREADY_REQUESTED, NODE_TX_ALREADY_REQUESTED, NODE_TIME_TO_GEN, NODE_NEIGHBOURHOOD, NODE_NEIGHBOURHOOD_INV, \
    NODE_NEIGHBOURHOOD_STATS, MSGS, NODE_HEADERS_TO_REQUEST, NODE_TIME_TO_SEND = 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13

NODE_INV_RECEIVED_BLOCKS, NODE_INV_RECEIVED_TX = 0, 1

NEIGHBOURHOOD_KNOWN_BLOCKS, NEIGHBOURHOOD_KNOWN_TX, NEIGHBOURHOOD_TX_TO_SEND = 0, 1, 2

TOP_N_NODES, STATS = 0, 1

TOTAL_TLL, TOTAL_MSG_RECEIVED = 0, 1

INV_MSG, GETHEADERS_MSG, HEADERS_MSG, GETDATA_MSG, BLOCK_MSG, CMPCTBLOCK_MSG, GETBLOCKTXN_MSG, BLOCKTXN_MSG, TX_MSG, MISSING_TX, \
    ALL_INVS = 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10

BLOCK_TYPE, TX_TYPE = True, False

RECEIVED_INV, RELEVANT_INV, RECEIVED_GETDATA = 0, 1, 2

MINE, NOT_MINE = True, False

SENT, RECEIVED = 0, 1

RECEIVE_TX, RECEIVED_BLOCKTX = 0, 1

INV_BLOCK_ID, INV_BLOCK_TTL = 0, 1

TIME, INBOUND = 0, 1

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

    for i in xrange(len(blocks_created)):
        if blocks_created[i][BLOCK_HEIGHT] + 2 < highest_block and not isinstance(blocks_created[i][BLOCK_TX], int):
            for tx in blocks_created[i][BLOCK_TX]:
                for myself in xrange(nb_nodes):
                    if tx in nodeState[myself][NODE_INV][NODE_INV_RECEIVED_TX]:
                        del nodeState[myself][NODE_INV][NODE_INV_RECEIVED_TX][tx]
                    if tx in nodeState[myself][NODE_MEMPOOL]:
                        del nodeState[myself][NODE_MEMPOOL][tx]

                    for neighbour in nodeState[myself][NODE_NEIGHBOURHOOD]:
                        if tx in nodeState[myself][NODE_NEIGHBOURHOOD_INV][neighbour][NEIGHBOURHOOD_KNOWN_TX]:
                            del nodeState[myself][NODE_NEIGHBOURHOOD_INV][neighbour][NEIGHBOURHOOD_KNOWN_TX][tx]
                        if tx in nodeState[myself][NODE_NEIGHBOURHOOD_INV][neighbour][NEIGHBOURHOOD_TX_TO_SEND]:
                            del nodeState[myself][NODE_NEIGHBOURHOOD_INV][neighbour][NEIGHBOURHOOD_TX_TO_SEND][tx]

            for myself in xrange(nb_nodes):
                nodeState[myself][NODE_INV][NODE_INV_RECEIVED_BLOCKS][i] = None

            replace_block = list(blocks_created[i])
            tx_list = replace_block[BLOCK_TX]
            replace_block[BLOCK_TX] = len(replace_block[BLOCK_TX])
            blocks_created[i] = tuple(replace_block)
            del replace_block
            del tx_list
            gc.collect()
            if gc.garbage:
                gc.garbage[0].set_next(None)
                del gc.garbage[:]


def CYCLE(myself):
    global nodeState, blocks_mined_by_randoms

    # with churn the node might be gone
    if myself not in nodeState:
        return

    # show progress for one node
    if myself == 0 and nodeState[myself][CURRENT_CYCLE] % 600 == 0:
        improve_performance(nodeState[myself][CURRENT_CYCLE])
        value = datetime.datetime.fromtimestamp(time.time())
        #output.write('{} cycle: {} mempool size: {}\n'.format(value.strftime('%Y-%m-%d %H:%M:%S'), nodeState[myself][CURRENT_CYCLE], len(nodeState[myself][NODE_MEMPOOL])))
        #output.flush()
        print('{} cycle: {} mempool size: {}'.format(value.strftime('%Y-%m-%d %H:%M:%S'), nodeState[myself][CURRENT_CYCLE], len(nodeState[myself][NODE_MEMPOOL])))

    # If a node can generate transactions
    i = 0
    n = get_nb_of_tx_to_gen(myself, len(nodeState), nodeState[myself][CURRENT_CYCLE])
    while i < n:
        generate_new_tx(myself)
        i += 1

    # If the node can generate a block
    if nodeState[myself][NODE_TIME_TO_GEN] == -1:
        next_t_to_gen(myself)

    if nodeState[myself][NODE_TIME_TO_GEN] == nodeState[myself][CURRENT_CYCLE]:
        next_t_to_gen(myself)
        if myself in miners or (myself not in miners and random.random() < 0.01 and
                                blocks_mined_by_randoms < total_blocks_mined_by_randoms):
            if myself not in miners:
                blocks_mined_by_randoms += 1
            new_block = generate_new_block(myself)

            # Check if can send as cmpct or send through inv
            for target in nodeState[myself][NODE_NEIGHBOURHOOD]:
                if check_availability(myself, target, BLOCK_TYPE, new_block[BLOCK_PARENT_ID]):
                    sim.send(CMPCTBLOCK, target, myself, cmpctblock(new_block))
                    if (expert_log and 3600 < nodeState[myself][CURRENT_CYCLE] < nb_cycles - 3600) or not expert_log:
                        nodeState[myself][MSGS][CMPCTBLOCK_MSG] += 1
                    update_neighbourhood_inv(myself, target, BLOCK_TYPE, new_block[BLOCK_ID])

                else:
                    vInv = [(BLOCK_TYPE, new_block[BLOCK_ID])]
                    # TODO change this send header and inv of possible parents
                    sim.send(INV, target, myself, vInv)
                    if (expert_log and 3600 < nodeState[myself][CURRENT_CYCLE] < nb_cycles - 3600) or not expert_log:
                        nodeState[myself][MSGS][INV_MSG][SENT] += 1
            del new_block

    # Send new transactions either created or received
    broadcast_invs(myself)

    nodeState[myself][CURRENT_CYCLE] += 1
    # schedule next execution
    if nodeState[myself][CURRENT_CYCLE] < nb_cycles:
        sim.schedulleExecution(CYCLE, myself)


def INV(myself, source, vInv):
    global nodeState

    new_connection(myself, source)

    # logger.info("Node {} Received {} from {}".format(myself, msg1, source))

    ask_for = []
    headers_to_request = []
    is_tx = False
    for inv in vInv:
        if inv[INV_TYPE] == TX_TYPE:
            if not is_tx:
                if (expert_log and 3600 < nodeState[myself][CURRENT_CYCLE] < nb_cycles - 3600) or not expert_log:
                    nodeState[myself][MSGS][INV_MSG][RECEIVED] += 1
                is_tx = True

            if (expert_log and 3600 < nodeState[myself][CURRENT_CYCLE] < nb_cycles - 3600) or not expert_log:
                nodeState[myself][MSGS][ALL_INVS][RECEIVED_INV] += 1
            update_neighbourhood_inv(myself, source, TX_TYPE, inv[INV_CONTENT_ID])
            seen_tx = have_it(myself, TX_TYPE, inv[INV_CONTENT_ID])
            if not seen_tx and inv[INV_CONTENT_ID] not in nodeState[myself][NODE_TX_ALREADY_REQUESTED]:
                ask_for.append(inv)
                nodeState[myself][NODE_TX_ALREADY_REQUESTED].append(inv[INV_CONTENT_ID])
                if (expert_log and 3600 < nodeState[myself][CURRENT_CYCLE] < nb_cycles - 3600) or not expert_log:
                    nodeState[myself][MSGS][ALL_INVS][RELEVANT_INV] += 1

        elif inv[INV_TYPE] == BLOCK_TYPE:
            update_neighbourhood_inv(myself, source, BLOCK_TYPE, inv[INV_CONTENT_ID])
            seen_block = have_it(myself, BLOCK_TYPE, inv[INV_CONTENT_ID])
            if not seen_block:
                if get_header(myself, inv[INV_CONTENT_ID]) is None:
                    headers_to_request.append(inv[INV_CONTENT_ID])

        else:
            # logger.info("Node {} Received INV from {} with invalid inv type {}".format(myself, source, inv))
            raise ValueError('INV, else, node received invalid inv type. This condition is not coded')

    if ask_for:
        sim.send(GETDATA, source, myself, ask_for)
        if (expert_log and 3600 < nodeState[myself][CURRENT_CYCLE] < nb_cycles - 3600) or not expert_log:
            nodeState[myself][MSGS][GETDATA_MSG][SENT] += 1
        del ask_for

    if headers_to_request:
        sim.send(GETHEADERS, source, myself, headers_to_request)
        if (expert_log and 3600 < nodeState[myself][CURRENT_CYCLE] < nb_cycles - 3600) or not expert_log:
            nodeState[myself][MSGS][GETHEADERS_MSG] += 1
        del headers_to_request


def GETHEADERS(myself, source, get_headers):
    global nodeState

    headers_to_send = []
    for id in get_headers:
        block = get_block(myself, id)
        if block is not None:
            headers_to_send.append(get_block_header(block))
        else:
            raise ValueError('GETHEADERS, else, node received invalid headerID')

    sim.send(HEADERS, source, myself, headers_to_send)
    if (expert_log and 3600 < nodeState[myself][CURRENT_CYCLE] < nb_cycles - 3600) or not expert_log:
        nodeState[myself][MSGS][HEADERS_MSG] += 1
    del headers_to_send


def HEADERS(myself, source, headers):
    global nodeState

    new_connection(myself, source)

    # logger.info("Node {} Received {} from {}".format(myself, msg1, source))

    process_new_headers(myself, source, headers)
    # TODO process_new_headers might return without doing anything check if we should also return
    data_to_request = get_data_to_request(myself, source)
    if len(data_to_request) <= 16:
        # If is a new block in the main chain try and direct fetch
        sim.send(GETDATA, source, myself, data_to_request)
        if (expert_log and 3600 < nodeState[myself][CURRENT_CYCLE] < nb_cycles - 3600) or not expert_log:
            nodeState[myself][MSGS][GETDATA_MSG][SENT] += 1
        del data_to_request
    else:
        # Else rely on other means of download
        # TODO Fix this case still don't know how it's done
        # logger.info("Node {} received more than 16 headers from {} INVALID!!!".format(myself, source))
        raise ValueError('HEADERS, else, this condition is not coded')


def GETDATA(myself, source, requesting_data):
    global nodeState

    # logger.info("Node {} Received {} from {}".format(myself, msg1, source))
    is_tx = False
    for inv in requesting_data:
        if inv[INV_TYPE] == TX_TYPE:
            if not is_tx:
                if (expert_log and 3600 < nodeState[myself][CURRENT_CYCLE] < nb_cycles - 3600) or not expert_log:
                    nodeState[myself][MSGS][GETDATA_MSG][RECEIVED] += 1
                is_tx = True

            if (expert_log and 3600 < nodeState[myself][CURRENT_CYCLE] < nb_cycles - 3600) or not expert_log:
                nodeState[myself][MSGS][ALL_INVS][RECEIVED_GETDATA] += 1
            tx = get_transaction(myself, inv[INV_CONTENT_ID])
            if tx is not None:
                sim.send(TX, source, myself, tx)
                if (expert_log and 3600 < nodeState[myself][CURRENT_CYCLE] < nb_cycles - 3600) or not expert_log:
                    nodeState[myself][MSGS][TX_MSG] += 1
                update_neighbourhood_inv(myself, source, TX_TYPE, tx)

        elif inv[INV_TYPE] == BLOCK_TYPE:
            block = get_block(myself, inv[INV_CONTENT_ID])
            if block is not None:
                if block[BLOCK_GEN_NODE] != myself:
                    block = list(block)
                    block[BLOCK_TTL] = nodeState[myself][NODE_INV][NODE_INV_RECEIVED_BLOCKS][block[BLOCK_ID]] + 1
                    block = tuple(block)
                sim.send(BLOCK, source, myself, block)
                if (expert_log and 3600 < nodeState[myself][CURRENT_CYCLE] < nb_cycles - 3600) or not expert_log:
                    nodeState[myself][MSGS][BLOCK_MSG] += 1
                update_neighbourhood_inv(myself, source, BLOCK_TYPE, block[BLOCK_ID])
                del block
            else:
                # This shouldn't happen in a simulated scenario
                raise ValueError('GETDATA, MSG_BLOCK else, this condition is not coded and shouldn\'t happen')

        else:
            # This shouldn't happen in a simulated scenario
            raise ValueError('GETDATA, else, this condition is not coded and shouldn\'t happen')


def BLOCK(myself, source, block):
    global nodeState

    # logger.info("Node {} Received {} from {}".format(myself, msg1, source))

    if block[BLOCK_ID] in nodeState[myself][NODE_BLOCKS_ALREADY_REQUESTED]:
        nodeState[myself][NODE_BLOCKS_ALREADY_REQUESTED].remove(block[BLOCK_ID])

    if block[BLOCK_ID] in nodeState[myself][NODE_HEADERS_TO_REQUEST]:
        nodeState[myself][NODE_HEADERS_TO_REQUEST].remove(block[BLOCK_ID])

    process_block(myself, source, block)


def CMPCTBLOCK(myself, source, cmpctblock):
    global nodeState

    new_connection(myself, source)

    # logger.info("Node {} Received {} from {}".format(myself, msg1, source))

    in_mem_cmpctblock = get_cmpctblock(myself, cmpctblock[BLOCK_ID])
    if have_it(myself, BLOCK_TYPE, cmpctblock[BLOCK_ID]) or in_mem_cmpctblock:
        update_neighbour_statistics(myself, source, cmpctblock[BLOCK_TTL])
        update_neighbourhood_inv(myself, source, BLOCK_TYPE, cmpctblock[BLOCK_ID])
        return

    in_headers = get_header(myself, cmpctblock[BLOCK_ID])
    if in_headers is not None:
        nodeState[myself][NODE_HEADERS_TO_REQUEST].remove(cmpctblock[BLOCK_ID])

    # Check if we have all tx
    tx_to_request = []
    for tx_id in cmpctblock[BLOCK_TX]:
        tx = get_transaction(myself, tx_id)
        if tx is None:
            tx_to_request.append(tx_id)

    if tx_to_request:
        sim.send(GETBLOCKTXN, source, myself, (cmpctblock[BLOCK_ID], tx_to_request))
        if (expert_log and 3600 < nodeState[myself][CURRENT_CYCLE] < nb_cycles - 3600) or not expert_log:
            nodeState[myself][MSGS][GETBLOCKTXN_MSG] += 1
            nodeState[myself][MSGS][MISSING_TX] += len(tx_to_request)
        nodeState[myself][NODE_PARTIAL_BLOCKS].append(cmpctblock[BLOCK_ID])
        del tx_to_request
    else:
        process_block(myself, source, cmpctblock)


def GETBLOCKTXN(myself, source, tx_request):
    global nodeState

    # logger.info("Node {} Received {} from {}".format(myself, msg1, source))

    sim.send(BLOCKTXN, source, myself, tx_request)
    if (expert_log and 3600 < nodeState[myself][CURRENT_CYCLE] < nb_cycles - 3600) or not expert_log:
        nodeState[myself][MSGS][BLOCKTXN_MSG] += 1


def BLOCKTXN(myself, source, tx_requested):
    global nodeState

    # logger.info("Node {} Received {} from {}".format(myself, msg1, source))

    if have_it(myself, BLOCK_TYPE, tx_requested[0]):
        return

    for tx in tx_requested[1]:
        if tx in nodeState[myself][NODE_TX_ALREADY_REQUESTED]:
            nodeState[myself][NODE_TX_ALREADY_REQUESTED].remove(tx)

    process_block(myself, source, build_cmpctblock(myself, tx_requested))


def TX(myself, source, tx):
    global nodeState

    # logger.info("Node {} Received {} from {}".format(myself, msg1, source))

    if tx in nodeState[myself][NODE_TX_ALREADY_REQUESTED]:
        nodeState[myself][NODE_TX_ALREADY_REQUESTED].remove(tx)

    update_neighbourhood_inv(myself, source, TX_TYPE, tx)
    if not have_it(myself, TX_TYPE, tx):
        update_have_it(myself, TX_TYPE, tx)
        nodeState[myself][NODE_MEMPOOL][tx] = None
        if myself not in bad_miners:
            push_to_send(myself, tx, NOT_MINE)
        if tx_array:
            tx_created[tx][RECEIVE_TX] += 1


def next_t_to_gen(myself):
    global nodeState

    y = numpy.random.normal(0.6, 0.11)
    if y > 1:
        x = - 10 * numpy.log(1-0.99)
    elif y < 0:
        x = - 10 * numpy.log(1-0.01)
    else:
        x = - 10 * numpy.log(1-y)

    for tuple in values:
        if tuple[0] <= x < tuple[1]:
            nodeState[myself][NODE_TIME_TO_GEN] += tuple[2]
            return


def generate_new_block(myself):
    global nodeState, block_id, blocks_created, highest_block

    # First block or
    # Not first block which means getting highest block to be the parent
    tx_array = get_tx_to_block(myself)
    if nodeState[myself][NODE_CURRENT_BLOCK] is None:
        new_block = (block_id, -1, 0, nodeState[myself][CURRENT_CYCLE], myself, tx_array, 0, nodeState[myself][CURRENT_CYCLE])
    else:
        highest_blocke = get_block(myself, nodeState[myself][NODE_CURRENT_BLOCK])
        new_block = (block_id, highest_blocke[BLOCK_ID], highest_blocke[BLOCK_HEIGHT] + 1, nodeState[myself][CURRENT_CYCLE],
                     myself, tx_array, 0, nodeState[myself][CURRENT_CYCLE])

    # Store the new block
    blocks_created.append(new_block)
    nodeState[myself][NODE_INV][NODE_INV_RECEIVED_BLOCKS][new_block[BLOCK_ID]] = 0
    nodeState[myself][NODE_CURRENT_BLOCK] = new_block[BLOCK_ID]
    block_id += 1
    if new_block[BLOCK_HEIGHT] > highest_block:
        highest_block = new_block[BLOCK_HEIGHT]
    return new_block


def inc_tll(block):
    lst = list(block)
    lst[BLOCK_TTL] += 1
    to_ret = tuple(lst)
    del lst
    return to_ret


def get_block(myself, block_id):
    if not have_it(myself, BLOCK_TYPE, block_id):
        raise ValueError("get_block I don't have id: {}".format(block_id))

    for item in reversed(blocks_created):
        if item[0] == block_id:
            return item
    return None


def super_get_block(block_id):
    for item in reversed(blocks_created):
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
        return

    worst_score = -1
    worst_index = -1
    for i in range(0, len(nodeState[myself][NODE_NEIGHBOURHOOD_STATS][TOP_N_NODES])):
        node = nodeState[myself][NODE_NEIGHBOURHOOD_STATS][TOP_N_NODES][i]
        total_ttl = nodeState[myself][NODE_NEIGHBOURHOOD_STATS][STATS][node][TOTAL_TLL]
        total_msg = nodeState[myself][NODE_NEIGHBOURHOOD_STATS][STATS][node][TOTAL_MSG_RECEIVED]
        member_score = total_ttl/total_msg
        if member_score < score:
            continue
        elif member_score >= score and worst_score < member_score:
            worst_score = member_score
            worst_index = i
        else:
            continue

    if worst_index != -1:
        nodeState[myself][NODE_NEIGHBOURHOOD_STATS][TOP_N_NODES][worst_index] = source


def process_block(myself, source, block):
    global nodeState

    # Check if it's a new block
    if not have_it(myself, BLOCK_TYPE, block[BLOCK_ID]):
        update_have_it(myself, BLOCK_TYPE, [block[BLOCK_ID], block[BLOCK_TTL]])
        if nodeState[myself][NODE_CURRENT_BLOCK] is None or \
                block[BLOCK_HEIGHT] > get_block(myself, nodeState[myself][NODE_CURRENT_BLOCK])[BLOCK_HEIGHT]:
            nodeState[myself][NODE_CURRENT_BLOCK] = block[BLOCK_ID]
        next_t_to_gen(myself)

        # Remove tx from MEMPOOL and from vINV_TX_TO_SEND
        update_tx(myself, block)

        # Broadcast new block
        update_neighbour_statistics(myself, source, block[BLOCK_TTL])
        update_neighbourhood_inv(myself, source, BLOCK_TYPE, block[BLOCK_ID])
        for target in nodeState[myself][NODE_NEIGHBOURHOOD]:
            if target == source or check_availability(myself, target, BLOCK_TYPE, block[BLOCK_ID]):
                continue
            elif check_availability(myself, target, BLOCK_TYPE, block[BLOCK_PARENT_ID]):
                block_to_send = inc_tll(block)
                sim.send(CMPCTBLOCK, target, myself, cmpctblock(block_to_send))
                if (expert_log and 3600 < nodeState[myself][CURRENT_CYCLE] < nb_cycles - 3600) or not expert_log:
                    nodeState[myself][MSGS][CMPCTBLOCK_MSG] += 1
                update_neighbourhood_inv(myself, target, BLOCK_TYPE, block_to_send[BLOCK_ID])
            else:
                sim.send(HEADERS, target, myself, [get_block_header(block)])
                if (expert_log and 3600 < nodeState[myself][CURRENT_CYCLE] < nb_cycles - 3600) or not expert_log:
                    nodeState[myself][MSGS][HEADERS_MSG] += 1

    else:
        update_neighbour_statistics(myself, source, block[BLOCK_TTL])
        update_neighbourhood_inv(myself, source, BLOCK_TYPE, block[BLOCK_ID])


def update_tx(myself, block):
    global nodeState

    for tx in block[BLOCK_TX]:
        if tx in nodeState[myself][NODE_MEMPOOL]:
            del nodeState[myself][NODE_MEMPOOL][tx]

        for neighbour in nodeState[myself][NODE_NEIGHBOURHOOD]:
            update_neighbourhood_inv(myself, neighbour, TX_TYPE, tx)

        if tx in nodeState[myself][NODE_TX_ALREADY_REQUESTED]:
            nodeState[myself][NODE_TX_ALREADY_REQUESTED].remove(tx)


def cmpctblock(block):
    cmpct_tx = []
    for tx in block[BLOCK_TX]:
        cmpct_tx.append(tx)
    return block[BLOCK_ID], block[BLOCK_PARENT_ID], block[BLOCK_HEIGHT], block[BLOCK_TIMESTAMP], block[BLOCK_GEN_NODE], cmpct_tx,\
           block[BLOCK_TTL], block[BLOCK_RECEIVED_TS]


def get_cmpctblock(myself, block_id):
    if block_id in nodeState[myself][NODE_PARTIAL_BLOCKS]:
        return True
    return False


def build_cmpctblock(myself, block_and_tx):
    cmpctblock = super_get_block(block_and_tx[0])

    if tx_array:
        for tx in block_and_tx[1]:
            tx_created[tx][RECEIVED_BLOCKTX] += 1

    nodeState[myself][NODE_PARTIAL_BLOCKS].remove(cmpctblock[BLOCK_ID])

    return cmpctblock


def get_block_header(block):
    return block[BLOCK_ID], block[BLOCK_PARENT_ID]


def have_it(myself, type, id):
    global nodeState

    if type != BLOCK_TYPE and type != TX_TYPE:
        print("check_availability strange type {}".format(type))
        exit(-1)

    if (type == BLOCK_TYPE and id in nodeState[myself][NODE_INV][NODE_INV_RECEIVED_BLOCKS]) or \
            (type == TX_TYPE and id in nodeState[myself][NODE_INV][NODE_INV_RECEIVED_TX]):
        return True
    return False


def update_have_it(myself, type, id):
    global nodeState

    if type != BLOCK_TYPE and type != TX_TYPE:
        print("check_availability strange type {}".format(type))
        exit(-1)

    if type == BLOCK_TYPE and id[INV_BLOCK_ID] not in nodeState[myself][NODE_INV][NODE_INV_RECEIVED_BLOCKS]:
        nodeState[myself][NODE_INV][NODE_INV_RECEIVED_BLOCKS][id[INV_BLOCK_ID]] = id[INV_BLOCK_TTL]
        if id[INV_BLOCK_ID] in nodeState[myself][NODE_PARTIAL_BLOCKS]:
            nodeState[myself][NODE_PARTIAL_BLOCKS].remove(id[INV_BLOCK_ID])
    elif type == TX_TYPE and id not in nodeState[myself][NODE_INV][NODE_INV_RECEIVED_TX]:
        nodeState[myself][NODE_INV][NODE_INV_RECEIVED_TX][id] = None


def get_header(myself, header_id):
    for header in nodeState[myself][NODE_HEADERS_TO_REQUEST]:
        if header == header_id:
            return header

    return None


# Neighbourhood update and check functions
def update_neighbourhood_inv(myself, target, type, id):
    global nodeState

    if type != BLOCK_TYPE and type != TX_TYPE:
        print("check_availability strange type {}".format(type))
        exit(-1)

    if type == BLOCK_TYPE and id not in nodeState[myself][NODE_NEIGHBOURHOOD_INV][target][NEIGHBOURHOOD_KNOWN_BLOCKS]:
        if id == -1:
            return
        nodeState[myself][NODE_NEIGHBOURHOOD_INV][target][NEIGHBOURHOOD_KNOWN_BLOCKS].insert(id)
    elif type == TX_TYPE and id not in nodeState[myself][NODE_NEIGHBOURHOOD_INV][target][NEIGHBOURHOOD_KNOWN_TX]:
        nodeState[myself][NODE_NEIGHBOURHOOD_INV][target][NEIGHBOURHOOD_KNOWN_TX][id] = None
        if id in nodeState[myself][NODE_NEIGHBOURHOOD_INV][target][NEIGHBOURHOOD_TX_TO_SEND]:
            del nodeState[myself][NODE_NEIGHBOURHOOD_INV][target][NEIGHBOURHOOD_TX_TO_SEND][id]


def check_availability(myself, target, type, id):
    if type != BLOCK_TYPE and type != TX_TYPE:
        print("check_availability strange type {}".format(type))
        exit(-1)

    if (type == BLOCK_TYPE and id in nodeState[myself][NODE_NEIGHBOURHOOD_INV][target][NEIGHBOURHOOD_KNOWN_BLOCKS]) or \
            (type == TX_TYPE and id in nodeState[myself][NODE_NEIGHBOURHOOD_INV][target][NEIGHBOURHOOD_KNOWN_TX]):
        return True
    return False


def push_to_send(myself, id, mine):
    global nodeState

    if mine and hop_based_broadcast and early_push:
        nodes_to_send = nodeState[myself][NODE_NEIGHBOURHOOD]
    else:
        nodes_to_send = get_nodes_to_send(myself)

    for node in nodes_to_send:
        if not check_availability(myself, node, TX_TYPE, id) and \
                id not in nodeState[myself][NODE_NEIGHBOURHOOD_INV][node][NEIGHBOURHOOD_TX_TO_SEND]:
            nodeState[myself][NODE_NEIGHBOURHOOD_INV][node][NEIGHBOURHOOD_TX_TO_SEND][id] = None
# -----------------------


def generate_new_tx(myself):
    global nodeState, tx_id

    new_tx = tx_id
    nodeState[myself][NODE_INV][NODE_INV_RECEIVED_TX][new_tx] = None
    nodeState[myself][NODE_MEMPOOL][new_tx] = None
    push_to_send(myself, new_tx, MINE)

    if tx_array:
        tx_created.append([0, 0])
    tx_id += 1


def get_transaction(myself, tx_id):
    if tx_id in nodeState[myself][NODE_MEMPOOL]:
        return tx_id
    return None


def get_tx_in_block(block, tx_id):
    if tx_id in block[BLOCK_TX]:
        return tx_id
    return None


def get_nb_of_tx_to_gen(myself, size, cycle):
    if myself in nodes_to_gen_tx[cycle]:
        return 1
    return 0


def get_tx_to_block(myself):
    global nodeState

    size = 0
    tx_array = []
    list_to_iter = dict(nodeState[myself][NODE_MEMPOOL])
    for tx in list_to_iter:
        if size + 700 <= max_block_size:
            size += 700
            tx_array.append(tx)
            del nodeState[myself][NODE_MEMPOOL][tx]
            for neighbour in nodeState[myself][NODE_NEIGHBOURHOOD]:
                if tx in nodeState[myself][NODE_NEIGHBOURHOOD_INV][neighbour][NEIGHBOURHOOD_TX_TO_SEND]:
                    del nodeState[myself][NODE_NEIGHBOURHOOD_INV][neighbour][NEIGHBOURHOOD_TX_TO_SEND][tx]
        elif size + min_tx_size > max_block_size:
            break
        else:
            continue
    del list_to_iter
    return tx_array


def update_time_to_send(myself, target):
    global nodeState

    current_cycle = nodeState[myself][CURRENT_CYCLE]
    if nodeState[myself][NODE_TIME_TO_SEND][target][INBOUND]:
        time_increment = poisson_send(current_cycle, 5)
    else:
        time_increment = poisson_send(current_cycle, 2.5)

    nodeState[myself][NODE_TIME_TO_SEND][target][TIME] = time_increment


def poisson_send(cycle, avg_inc):
    if avg_inc == 5:
        return cycle + 5 * random.randrange(3, 5) # 70
    else:
        return cycle + 2.5* random.randrange(1, 5) # 70


def broadcast_invs(myself):
    global nodeState

    current_cycle = nodeState[myself][CURRENT_CYCLE]
    for target in nodeState[myself][NODE_NEIGHBOURHOOD]:
        time_to_send = nodeState[myself][NODE_TIME_TO_SEND][target][TIME]
        if current_cycle > time_to_send and \
                len(nodeState[myself][NODE_NEIGHBOURHOOD_INV][target][NEIGHBOURHOOD_TX_TO_SEND]) > 0:
            update_time_to_send(myself, target)
            inv_to_send = []
            copy = dict(nodeState[myself][NODE_NEIGHBOURHOOD_INV][target][NEIGHBOURHOOD_TX_TO_SEND])
            counter = 0
            for tx in copy:
                if counter > 35:
                    break
                if not check_availability(myself, target, TX_TYPE, tx):
                    inv_to_send.append((TX_TYPE, tx))
                    update_neighbourhood_inv(myself, target, TX_TYPE, tx)
                    counter += 1
            del copy
            sim.send(INV, target, myself, inv_to_send)
            if (expert_log and 3600 < nodeState[myself][CURRENT_CYCLE] < nb_cycles - 3600) or not expert_log:
                nodeState[myself][MSGS][INV_MSG][SENT] += 1


def get_nodes_to_send(myself):
    if not hop_based_broadcast or not nodeState[myself][NODE_NEIGHBOURHOOD_STATS][TOP_N_NODES]:
        return nodeState[myself][NODE_NEIGHBOURHOOD]

    total = top_nodes_size + random_nodes_size
    top_nodes = nodeState[myself][NODE_NEIGHBOURHOOD_STATS][TOP_N_NODES]
    if len(nodeState[myself][NODE_NEIGHBOURHOOD]) < total:
        total = len(nodeState[myself][NODE_NEIGHBOURHOOD]) - len(top_nodes)
    else:
        total = total - len(top_nodes)

    random_nodes = []
    if total > 0:
        collection_of_neighbours = list(nodeState[myself][NODE_NEIGHBOURHOOD])
        for node in top_nodes:
            if node in collection_of_neighbours:
                collection_of_neighbours.remove(node)
        random_nodes = random.sample(collection_of_neighbours, total)
        del collection_of_neighbours

    return top_nodes + random_nodes


def process_new_headers(myself, source, headers):
    global nodeState

    for header in headers:
        seen_block = have_it(myself, BLOCK_TYPE, header[HEADER_ID])
        have_header = None
        if not seen_block:
            have_header = get_header(myself, header[HEADER_ID])

        seen_parent = have_it(myself, BLOCK_TYPE, header[HEADER_PARENT_ID])
        update_neighbourhood_inv(myself, source, BLOCK_TYPE, header[HEADER_ID])
        update_neighbourhood_inv(myself, source, BLOCK_TYPE, header[HEADER_PARENT_ID])

        if not seen_parent and header[HEADER_PARENT_ID] != -1:
            # TODO REFACTOR
            #logger.info("Node {} Received a header with a parent that doesn't connect id={} THIS NEEDS TO BE CODED!!"
            #            .format(myself, header[HEADER_PARENT_ID]))
            headers_to_request = [header[HEADER_PARENT_ID], header[HEADER_ID]]
            sim.send(GETHEADERS, source, myself, headers_to_request)
            if (expert_log and 3600 < nodeState[myself][CURRENT_CYCLE] < nb_cycles - 3600) or not expert_log:
                nodeState[myself][MSGS][GETHEADERS_MSG] += 1
            continue

        elif (seen_parent or header[HEADER_PARENT_ID] == -1) and (not seen_block and have_header is None):
            nodeState[myself][NODE_HEADERS_TO_REQUEST].append(header[HEADER_ID])


def get_data_to_request(myself, source):
    global nodeState

    data_to_request = []
    for header_id in nodeState[myself][NODE_HEADERS_TO_REQUEST]:
        if check_availability(myself, source, BLOCK_TYPE, header_id) and \
                header_id not in nodeState[myself][NODE_BLOCKS_ALREADY_REQUESTED]:
            data_to_request.append((BLOCK_TYPE, header_id))
            nodeState[myself][NODE_BLOCKS_ALREADY_REQUESTED].append(header_id)

    return data_to_request


def new_connection(myself, source):
    global nodeState

    if len(nodeState[myself][NODE_NEIGHBOURHOOD]) > 125:
        raise ValueError("Number of connections in one node exceed the maximum allowed")

    if source in nodeState[myself][NODE_NEIGHBOURHOOD]:
        return
    else:
        nodeState[myself][NODE_NEIGHBOURHOOD].append(source)
        nodeState[myself][NODE_NEIGHBOURHOOD_INV][source] = [SortedCollection(), defaultdict(), defaultdict()]
        nodeState[myself][NODE_NEIGHBOURHOOD_STATS][STATS][source] = [0, 0]
        nodeState[myself][NODE_TIME_TO_SEND][source] = [poisson_send(nodeState[myself][CURRENT_CYCLE], 5), True]


def get_all_genesis():
    genesis = []
    for block in blocks_created:
        if block[BLOCK_HEIGHT] == 0 and block[BLOCK_ID] not in genesis:
            genesis.append(block[BLOCK_ID])
    return genesis


def fork_rate():
    branches = get_all_genesis()
    all_blocks = list(range(0, block_id))
    i = 0

    while len(all_blocks) != 0:
        current_block = branches[i]
        all_blocks.remove(current_block)
        found = False

        for potential_block in blocks_created:
            if potential_block[BLOCK_PARENT_ID] == current_block and potential_block[BLOCK_ID] not in branches:
                found = True
                branches.append(potential_block[BLOCK_ID])

        if len(all_blocks) == 0:
            break

        if not found:
            i += 1
        elif found:
            branches.pop(i)

    return len(branches)


def get_miner_hops():
    seen = {}
    depth = 0
    for miner in miners:
        seen[miner] = depth

    to_call = list(miners)
    called = list(miners)
    seen = count_hops(to_call, called, seen, depth)

    further = numpy.amax(seen.values())
    counter = [0] * (further + 1)
    for node in seen.keys():
        counter[seen[node]] += 1

    return counter


def count_hops(to_call, called, seen, depth):
    if len(to_call) == 0:
        return seen

    dup_to_call = list(to_call)
    for calling in dup_to_call:
        called.append(calling)
        to_call.remove(calling)
        if calling not in seen.keys() or seen[calling] > depth:
            seen[calling] = depth

        for neighbour in nodeState[calling][NODE_NEIGHBOURHOOD]:
            if neighbour not in called and neighbour not in to_call:
                to_call.append(neighbour)

    return count_hops(to_call, called, seen, depth + 1)


def get_avg_tx_per_block():
    total_num_if_tx = 0
    blocks_not_counted = 0
    for block in blocks_created:
        if (expert_log and 3600 < block[BLOCK_TIMESTAMP] < nb_cycles - 3600) or not expert_log:
            if isinstance(block[BLOCK_TX], int):
                total_num_if_tx += block[BLOCK_TX]
            else:
                total_num_if_tx += len(block[BLOCK_TX])
        elif expert_log:
            blocks_not_counted += 1

    return total_num_if_tx/(block_id - blocks_not_counted)


def get_avg_total_sent_msg():
    total_sent = [0] * nb_nodes
    for node in xrange(nb_nodes):
        for i in range(INV_MSG, MISSING_TX):
            if i == 0 or i == 3:
                total_sent[node] += nodeState[node][MSGS][i][SENT]
            else:
                total_sent[node] += nodeState[node][MSGS][i]

    total_sent = sum(total_sent)

    return total_sent/nb_nodes


def wrapup():
    global nodeState
    #logger.info("Wrapping up")
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
    all_inv = map(lambda x: nodeState[x][MSGS][ALL_INVS][RECEIVED_INV], nodeState)
    relevant_inv = map(lambda x: nodeState[x][MSGS][ALL_INVS][RELEVANT_INV], nodeState)
    all_getdata = map(lambda x: nodeState[x][MSGS][ALL_INVS][RECEIVED_GETDATA], nodeState)

    sum_received_blocks = map(lambda x: nodeState[x][NODE_INV][NODE_INV_RECEIVED_BLOCKS], nodeState)
    #receivedBlocks = map(lambda x: map(lambda y: (sum_received_blocks[x][y][0], sum_received_blocks[x][y][1],
    #                                              sum_received_blocks[x][y][2], sum_received_blocks[x][y][3],
    #                                              sum_received_blocks[x][y][4], sum_received_blocks[x][y][6]),
    #                                   xrange(len(sum_received_blocks[x]))), nodeState)

    # dump data into gnuplot format
    utils.dump_as_gnu_plot([inv_messages, getheaders_messages, headers_messages, getdata_messages, block_messages,
                         cmpctblock_messages, getblocktx_messages, blocktx_messages, tx_messages, sum_received_blocks],
                        dumpPath + '/messages-' + str(runId) + '.gpData',
                        ['inv getheaders headers getdata block cmpctblock getblocktx blocktx tx'
                         '           sum_received_blocks                    receivedBlocks'])

    sum_inv = 0
    sum_getData = 0
    sum_tx = 0
    sum_getBlockTX = 0
    sum_missingTX = 0
    sum_all_inv = 0
    sum_relevant_inv = 0
    sum_received_invs = 0
    sum_received_getdata = 0
    sum_all_getdata = 0
    for i in range(0, nb_nodes):
        sum_inv += inv_messages[i][SENT]
        sum_received_invs += inv_messages[i][RECEIVED]
        sum_getData += getdata_messages[i][SENT]
        sum_received_getdata += getdata_messages[i][RECEIVED]
        sum_tx += tx_messages[i]
        sum_getBlockTX += getblocktx_messages[i]
        sum_missingTX += missing_tx[i]
        sum_all_inv += all_inv[i]
        sum_relevant_inv += relevant_inv[i]
        sum_all_getdata += all_getdata[i]

    #avg_block_diss = avg_block_dissemination()
    nb_forks = fork_rate()
    hops_distribution = get_miner_hops()
    avg_tx_per_block = get_avg_tx_per_block()
    avg_total_sent_msg = get_avg_total_sent_msg()
    # ---------
    avg_duplicated_inv = sum_all_inv/sum_relevant_inv
    # ---------
    avg_entries_per_inv = sum_all_inv/sum_received_invs
    avg_entries_per_getdata = sum_all_getdata/sum_received_getdata

    print(nodeState[nb_nodes-1][MSGS][ALL_INVS][RECEIVED_INV] / nodeState[nb_nodes-1][MSGS][ALL_INVS][RELEVANT_INV])

    first_time = not os.path.isfile('out/{}.csv'.format(results_name))
    if first_time:
        csv_file_to_write = open('out/results.csv', 'w')
        spam_writer = csv.writer(csv_file_to_write, delimiter=',', quotechar='\'', quoting=csv.QUOTE_MINIMAL)
        spam_writer.writerow(["Number of nodes", "Number of cycles", "Number of miners", "Extra miners"])
        spam_writer.writerow([nb_nodes, nb_cycles, number_of_miners, extra_replicas])
        spam_writer.writerow(["Top nodes size", "Random nodes size", "Early push", "Bad miners", "Avg inv", "Avg entries per inv",
                              "Avg getData", "Avg entries per getData", "Avg Tx", "Avg getBlockTX",
                              "Avg missing tx", "Avg numb of tx per block", "% of duplicates inv", "Avg total sent messages",
                              "Total number of branches", "Hops distribution"])
    else:
        csv_file_to_write = open('out/results.csv', 'a')
        spam_writer = csv.writer(csv_file_to_write, delimiter=',', quotechar='\'', quoting=csv.QUOTE_MINIMAL)

    if not hop_based_broadcast:
        spam_writer.writerow(["False", "False", early_push, number_of_bad_miners, sum_inv / nb_nodes, avg_entries_per_inv,
                              sum_getData / nb_nodes,
                              avg_entries_per_getdata, sum_tx / nb_nodes, sum_getBlockTX / nb_nodes,
                              sum_missingTX / nb_nodes, avg_tx_per_block, avg_duplicated_inv,
                              avg_total_sent_msg, nb_forks, ''.join(str(e) + " " for e in hops_distribution)])
    else:
        spam_writer.writerow([top_nodes_size, random_nodes_size, early_push, number_of_bad_miners, sum_inv / nb_nodes,
                              avg_entries_per_inv,
                              sum_getData / nb_nodes, avg_entries_per_getdata, sum_tx / nb_nodes,
                              sum_getBlockTX / nb_nodes, sum_missingTX / nb_nodes, avg_tx_per_block,
                              avg_duplicated_inv, avg_total_sent_msg, nb_forks,
                              ''.join(str(e) + " " for e in hops_distribution)])
    csv_file_to_write.flush()
    csv_file_to_write.close()
    print(tx_id)


def save_network():
    with open('networks/{}-{}-{}'.format(nb_nodes-(number_of_miners*extra_replicas), number_of_miners, extra_replicas), 'w') \
            as file_to_write:
        file_to_write.write("{} {} {}\n".format(nb_nodes, number_of_miners, extra_replicas))
        for n in xrange(nb_nodes):
            file_to_write.write(str(nodeState[n][NODE_NEIGHBOURHOOD]) + '\n')
        file_to_write.write(str(miners) + '\n')
        file_to_write.write(str(bad_miners) + '\n')


def load_network(filename):
    global nodeState, nb_nodes, number_of_miners, extra_replicas, miners, bad_miners

    if filename == "":
        raise ValueError("No file named inputted in not create new run")

    with open('networks/' + filename, 'r') as file_to_read:
        first_line = file_to_read.readline()
        nb_nodes, number_of_miners, extra_replicas = first_line.split()
        nb_nodes, number_of_miners, extra_replicas = int(nb_nodes), int(number_of_miners), int(extra_replicas)
        nodeState = defaultdict()
        for n in xrange(nb_nodes):
            nodeState[n] = createNode(ast.literal_eval(file_to_read.readline()))
        miners = ast.literal_eval(file_to_read.readline())
        bad_miners = ast.literal_eval(file_to_read.readline())


def create_bad_miner():
    global bad_miners

    bad_miners = random.sample(miners, number_of_bad_miners)


def create_special_node():
    global nb_nodes

    n = nb_nodes
    random_nodes = random.sample(xrange(nb_nodes), 2)
    neighbourhood = random.sample(xrange(nb_nodes), 8)
    for node in random_nodes:
        nodeState[node][NODE_NEIGHBOURHOOD].pop()
        nodeState[node][NODE_NEIGHBOURHOOD].append(n)
        nodeState[node][NODE_NEIGHBOURHOOD_INV][n] = [SortedCollection(), defaultdict(), defaultdict()]
        nodeState[node][NODE_NEIGHBOURHOOD_STATS][STATS][n] = [0, 0]
        nodeState[node][NODE_TIME_TO_SEND][n] = [poisson_send(0, 2.5), False]

    nodeState[n] = createNode(neighbourhood)
    nb_nodes += 1
    return


def create_network(create_new, save_network_connections, neighbourhood_size, filename=""):
    global nb_nodes, nodeState, miners

    first_time = not os.path.exists("networks/")

    if first_time:
        os.makedirs("networks/")

    if first_time or create_new:
        create_nodes_and_miners(neighbourhood_size)
        create_miner_replicas(neighbourhood_size)
        create_bad_miner()
        create_special_node()
        if save_network_connections:
            save_network()
    else:
        load_network(filename)


def createNode(neighbourhood):
    current_cycle = 0
    node_current_block = None
    node_inv = [defaultdict(), defaultdict()]
    node_partial_blocks = []
    node_mempool = defaultdict()
    node_blocks_already_requested = []
    node_tx_already_requested = []
    node_time_to_gen = -1
    node_neighbourhood_inv = defaultdict()
    stats = defaultdict()
    time_to_send = defaultdict()
    topx = []
    node_headers_requested = []
    for neighbour in neighbourhood:
        node_neighbourhood_inv[neighbour] = [SortedCollection(), defaultdict(), defaultdict()]
        stats[neighbour] = [0, 0]
        time_to_send[neighbour] = [poisson_send(0, 2.5), False]
    node_neighbourhood_stats = [topx, stats]

    msgs = [[0, 0], 0, 0, [0, 0], 0, 0, 0, 0, 0, 0, [0, 0, 0]]

    return [current_cycle, node_current_block, node_inv, node_partial_blocks, node_mempool,
            node_blocks_already_requested, node_tx_already_requested, node_time_to_gen, neighbourhood,
            node_neighbourhood_inv, node_neighbourhood_stats, msgs, node_headers_requested, time_to_send]


def create_nodes_and_miners(neighbourhood_size):
    global nodeState, miners

    nodeState = defaultdict()
    for n in xrange(nb_nodes):
        neighbourhood = random.sample(xrange(nb_nodes), neighbourhood_size)
        while neighbourhood.__contains__(n):
            neighbourhood = random.sample(xrange(nb_nodes), neighbourhood_size)
        nodeState[n] = createNode(neighbourhood)

    miners = random.sample(xrange(nb_nodes), number_of_miners)


def create_miner_replicas(neighbourhood_size):
    global nb_nodes, nodeState, miners

    if extra_replicas > 0:
        i = 0
        miners_to_add = []
        for n in xrange(nb_nodes, nb_nodes + (extra_replicas * number_of_miners)):
            neighbourhood = random.sample(xrange(nb_nodes), neighbourhood_size)
            while neighbourhood.__contains__(n) or neighbourhood.__contains__(miners[i]):
                neighbourhood = random.sample(xrange(nb_nodes), neighbourhood_size)
            neighbourhood.append(miners[i])
            nodeState[n] = createNode(neighbourhood)
            miners_to_add.append(n)
            i += 1
            if i == number_of_miners - 1:
                i = 0
        miners = miners + miners_to_add

        nb_nodes = nb_nodes + (extra_replicas * number_of_miners)


def configure(config):
    global nb_nodes, nb_cycles, nodeState, node_cycle, block_id, tx_id, \
        number_of_tx_to_gen_per_cycle, max_block_size, min_tx_size, max_tx_size, values, nodes_to_gen_tx, miners, \
        top_nodes_size, hop_based_broadcast, number_of_miners, extra_replicas, blocks_created, blocks_mined_by_randoms, \
        total_blocks_mined_by_randoms, highest_block, random_nodes_size, tx_created, tx_array, expert_log, bad_miners, \
        number_of_bad_miners


    node_cycle = int(config['NODE_CYCLE'])

    nb_nodes = config['NUMBER_OF_NODES']
    neighbourhood_size = int(config['NEIGHBOURHOOD_SIZE'])

    if top_nodes != -1:
        if top_nodes == 0:
            hop_based_broadcast = False
        else:
            hop_based_broadcast = True
        top_nodes_size = top_nodes
        if random_nodes != -1:
            random_nodes_size = random_nodes
        else:
            random_nodes_size = top_nodes
    else:
        top_nodes_size = int(config['TOP_NODES_SIZE'])
        hop_based_broadcast = bool(config['HOP_BASED_BROADCAST'])

    if number_of_bad_miners == 0:
        number_of_bad_miners = int(config['NUMBER_OF_BAD_MINERS'])

    number_of_miners = int(config['NUMBER_OF_MINERS'])
    extra_replicas = int(config['EXTRA_REPLICAS'])

    nb_cycles = config['NUMBER_OF_CYCLES']
    max_block_size = int(config['MAX_BLOCK_SIZE'])

    tx_array = bool(config['TX_ARRAY'])
    number_of_tx_to_gen_per_cycle = config['NUMB_TX_PER_CYCLE']
    min_tx_size = int(config['MIN_TX_SIZE'])
    max_tx_size = int(config['MAX_TX_SIZE'])
    blocks_mined_by_randoms = 0
    total_blocks_mined_by_randoms = (nb_cycles/10) * 0.052

    expert_log = bool(config['EXPERT_LOG'])
    if expert_log == True:
        if nb_cycles <= 7200:
            raise ValueError("With expert_log activated you have to complete more than 120 cycles")

    block_id = 0
    blocks_created = []
    highest_block = -1
    tx_id = 0
    tx_created = []

    values = []
    i = -1
    j = 20*60
    while i < 20:
        values.append((i, i + 1, j))
        i += 1
        j -= 60

    create_network(create_new, save_network_connections, neighbourhood_size, file_name)

    if number_of_tx_to_gen_per_cycle//nb_nodes == 0:
        nodes_to_gen_tx = []
        for i in range(0, nb_cycles):
            value = random.random()
            if value < 0.3:
                number_of_tx = number_of_tx_to_gen_per_cycle + 1
            else:
                number_of_tx = number_of_tx_to_gen_per_cycle

            nodes_to_gen_tx.append(random.sample(xrange(nb_nodes), number_of_tx))

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

    top_nodes = -1
    random_nodes = -1
    create_new = True
    early_push = False
    save_network_connections = False
    file_name = ""
    results_name = "results"
    number_of_bad_miners = 0
    if len(sys.argv) > 3:
        i = 3
        while i < len(sys.argv):
            if sys.argv[i] == "-cn":
                create_new = sys.argv[i+1]
            elif sys.argv[i] == "-sn":
                save_network_connections = sys.argv[i+1]
            elif sys.argv[i] == "-tn":
                top_nodes = int(sys.argv[i+1])
            elif sys.argv[i] == "-rn":
                random_nodes = int(sys.argv[i+1])
            elif sys.argv[i] == "-ln":
                create_new = False
                save_network_connections = False
                file_name = sys.argv[i+1]
            elif sys.argv[i] == "-rsn":
                results_name = sys.argv[i+1]
            elif sys.argv[i] == "-ep":
                early_push = sys.argv[i+1]
            elif sys.argv[i] == "-bm":
                number_of_bad_miners = int(sys.argv[i+1])
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
