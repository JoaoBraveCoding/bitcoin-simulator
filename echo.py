# Sample simulator demo
# Miguel Matos - miguel.marques.matos@tecnico.ulisboa.pt
# (c) 2012-2018
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

BLOCK_ID, BLOCK_PARENT_ID, BLOCK_HEIGHT, BLOCK_TIMESTAMP, BLOCK_GEN_NODE, BLOCK_TX, BLOCK_EXTRA_TX = 0, 1, 2, 3, 4, 5, 6
TX_ID, TX_CONTENT, TX_GEN_NODE, TX_SIZE = 0, 1, 2, 3
INV_TYPE, INV_CONTENT_ID = 0, 1
HEADER_ID, HEADER_PARENT_ID, HEADER_TIMESTAMP, HEADER_GEN_NODE = 0, 1, 2, 3

CURRENT_CYCLE, INV_MSG, GETHEADERS_MSG, HEADERS_MSG, GETDATA_MSG, BLOCK_MSG, CMPCTBLOCK_MSG, GETBLOCKTXN_MSG, BLOCKTXN_MSG, \
TX_MSG, NODE_CURRENT_BLOCK, NODE_NEIGHBOURHOOD, NODE_RECEIVED_BLOCKS, NODE_PARTIAL_BLOCKS, NODE_BLOCKS_AVAILABILITY, \
NODE_MEMPOOL, NODE_vINV_TX_TO_SEND, NODE_BLOCKS_ALREADY_REQUESTED, NODE_TX_ALREADY_REQUESTED, NODE_TIME_TO_GEN \
    = 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19

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
    n = get_nb_of_tx_to_gen(len(nodeState), nodeState[myself][CURRENT_CYCLE])
    while i < n:
        generate_new_tx(myself)
        i = i + 1

    # If the node can generate a block
    if nodeState[myself][NODE_TIME_TO_GEN] == -1:
        next_t_to_gen(myself)

    if nodeState[myself][NODE_TIME_TO_GEN] == nodeState[myself][CURRENT_CYCLE] and (max_block_number == 0 or block_id < max_block_number):
        next_t_to_gen(myself)
        new_block = generate_new_block(myself)

        # Check if can send as cmpct or send through inv
        for target in nodeState[myself][NODE_NEIGHBOURHOOD]:
            if nodeState[myself][NODE_CURRENT_BLOCK] is not None and \
                    check_availability(myself, target, nodeState[myself][NODE_CURRENT_BLOCK][BLOCK_PARENT_ID]):
                sim.send(CMPCTBLOCK, target, myself, "CMPCTBLOCK", cmpctblock(new_block))
                nodeState[myself][CMPCTBLOCK_MSG][SENT] += 1
                nodeState[myself][NODE_BLOCKS_AVAILABILITY].setdefault(target, []).append(new_block)

            else:
                vInv = [("MSG_BLOCK", new_block[BLOCK_ID])]
                sim.send(INV, target, myself, "INV", vInv)
                nodeState[myself][INV_MSG][SENT] += 1

    # Send new transactions either created or received
    if nodeState[myself][NODE_vINV_TX_TO_SEND]:
        for target in nodeState[myself][NODE_NEIGHBOURHOOD]:
            sim.send(INV, target, myself, "INV", nodeState[myself][NODE_vINV_TX_TO_SEND])
            nodeState[myself][INV_MSG][SENT] += 1

        nodeState[myself][NODE_vINV_TX_TO_SEND] = []

    nodeState[myself][CURRENT_CYCLE] += 1

    # schedule next execution
    if nodeState[myself][CURRENT_CYCLE] < nbCycles:
        sim.schedulleExecution(CYCLE, myself)


def INV(myself, source, msg1, vInv):
    global nodeState

    # logger.info("Node {} Received {} from {}".format(myself, msg1, source))
    nodeState[myself][INV_MSG][RECEIVED] += 1

    ask_for = []
    headers_to_request = []
    for inv in vInv:
        if inv[INV_TYPE] == "MSG_TX":
            tx = get_transaction(myself, inv[INV_CONTENT_ID])
            if tx is None and inv[INV_CONTENT_ID] not in nodeState[myself][NODE_TX_ALREADY_REQUESTED]:
                ask_for.append(inv)
                nodeState[myself][NODE_TX_ALREADY_REQUESTED].append(inv[INV_CONTENT_ID])
            elif tx is None:
                continue

        elif inv[INV_TYPE] == "MSG_BLOCK":
            block = get_block(myself, inv[INV_CONTENT_ID])
            if block is None:
                headers_to_request.append(inv[INV_CONTENT_ID])
                update_availability(myself, source, (inv[INV_CONTENT_ID],))
            else:
                update_availability(myself, source, block)
        else:
            # logger.info("Node {} Received INV from {} with invalid inv type {}".format(myself, source, inv))
            raise ValueError('INV, else, node received invalid inv type. This condition is not coded')

    if ask_for:
        sim.send(GETDATA, source, myself, "GETDATA", ask_for)
        nodeState[myself][GETDATA_MSG][SENT] += 1

    if headers_to_request:
        sim.send(GETHEADERS, source, myself, "GETHEADERS", headers_to_request)
        nodeState[myself][GETHEADERS_MSG][SENT] += 1


def GETHEADERS(myself, source, msg1, get_headers):
    global nodeState

    # logger.info("Node {} Received {} from {}".format(myself, msg1, source))
    nodeState[myself][GETHEADERS_MSG][RECEIVED] += 1

    headers_to_send = []
    for id in get_headers:
        block = get_block(myself, id)
        if block is not None:
            headers_to_send.append(get_block_header(block))
        else:
            # logger.info("Node {} Received header from {} INVALID ID in header!!!".format(myself, source))
            raise ValueError('GETHEADERS, else, node received invalid headerID')

    sim.send(HEADERS, source, myself, "HEADERS", headers_to_send)
    nodeState[myself][HEADERS_MSG][SENT] += 1


def HEADERS(myself, source, msg1, headers):
    global nodeState

    # logger.info("Node {} Received {} from {}".format(myself, msg1, source))
    nodeState[myself][HEADERS_MSG][RECEIVED] += 1

    process_new_headers(myself, source, headers)
    # TODO process_new_headers might return without doing anything check if we should also return
    data_to_request = get_data_to_request(myself, source)
    if len(data_to_request) <= 16:
        # If is a new block in the main chain try and direct fetch
        sim.send(GETDATA, source, myself, "GETDATA", data_to_request)
        nodeState[myself][GETDATA_MSG][SENT] += 1
    else:
        # Else rely on other means of download
        # TODO Fix this case still don't know how it's done
        # logger.info("Node {} received more than 16 headers from {} INVALID!!!".format(myself, source))
        raise ValueError('HEADERS, else, this condition is not coded')


def GETDATA(myself, source, msg1, requesting_data):
    global nodeState

    # logger.info("Node {} Received {} from {}".format(myself, msg1, source))
    nodeState[myself][GETDATA_MSG][RECEIVED] += 1

    for inv in requesting_data:
        if inv[INV_TYPE] == "MSG_TX":
            tx = get_transaction(myself, inv[INV_CONTENT_ID])
            if tx is not None:
                sim.send(TX, source, myself, "TX", tx)
                nodeState[myself][TX_MSG][SENT] += 1
            # else:
                # This shouldn't happen in a simulated scenario
                # logger.info(
                #    "Node {} Received more invalid inv_id for a transation in GETDATA from {} INVALID!!!".format(myself, source))
                # raise ValueError('GETDATA, MSG_TX else, this condition is not coded and shouldn\'t happen')

        elif inv[INV_TYPE] == "MSG_BLOCK":
            block = get_block(myself, inv[INV_CONTENT_ID])
            if block is not None:
                sim.send(BLOCK, source, myself, "BLOCK", block)
                nodeState[myself][BLOCK_MSG][SENT] += 1
                update_availability(myself, source, block)
            else:
                # This shouldn't happen in a simulated scenario
                # logger.info(
                #    "Node {} Received {} from {} with invalid block_id in GETDATA INVALID REQUEST".format(myself, msg1, source))
                raise ValueError('GETDATA, MSG_BLOCK else, this condition is not coded and shouldn\'t happen')

        else:
            # This shouldn't happen in a simulated scenario
            # logger.info("Node {} Received more invalid inv type in GETDATA from {} INVALID!!!".format(myself, source))
            raise ValueError('GETDATA, else, this condition is not coded and shouldn\'t happen')


def BLOCK(myself, source, msg1, block):
    global nodeState

    # logger.info("Node {} Received {} from {}".format(myself, msg1, source))
    nodeState[myself][BLOCK_MSG][RECEIVED] += 1

    if block[BLOCK_ID] in nodeState[myself][NODE_BLOCKS_ALREADY_REQUESTED]:
        nodeState[myself][NODE_BLOCKS_ALREADY_REQUESTED].remove(block[BLOCK_ID])

    process_block(myself, source, block)


def CMPCTBLOCK(myself, source, msg1, cmpctblock):
    global nodeState

    # logger.info("Node {} Received {} from {}".format(myself, msg1, source))
    nodeState[myself][CMPCTBLOCK_MSG][RECEIVED] += 1

    block = get_block(myself, cmpctblock[BLOCK_ID])
    if block is not None:
        # logger.info("Node {} block already built ignore BLOCKTX {}".format(myself, source))
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
        nodeState[myself][GETBLOCKTXN_MSG][SENT] += 1

        if cmpctblock not in nodeState[myself][NODE_PARTIAL_BLOCKS]:
            # TODO This could be optimized to use tx that we receive from other cmpct blocks
            nodeState[myself][NODE_PARTIAL_BLOCKS].append(cmpctblock[:BLOCK_EXTRA_TX])
        return

    process_block(myself, source, cmpctblock[:BLOCK_EXTRA_TX])


def GETBLOCKTXN(myself, source, msg1, tx_request):
    global nodeState

    # logger.info("Node {} Received {} from {}".format(myself, msg1, source))
    nodeState[myself][GETBLOCKTXN_MSG][RECEIVED] += 1

    block = get_block(myself, tx_request[0])
    tx_to_send = []
    for tx in block[BLOCK_TX]:
        if tx[TX_ID] in tx_request[1]:
            tx_to_send.append(tx)

    if len(tx_to_send) != len(tx_request[1]):
        # logger.info("Node {} Received invalid tx_id in GETBLOCKTXN from {} INVALID!!!".format(myself, source))
        raise ValueError('GETBLOCKTXN, else, this condition is not coded invalid tx_id')

    if tx_to_send:
        sim.send(BLOCKTXN, source, myself, "BLOCKTXN", (tx_request[0], tx_to_send))
        nodeState[myself][BLOCKTXN_MSG][SENT] += 1


def BLOCKTXN(myself, source, msg1, tx_requested):
    global nodeState

    # logger.info("Node {} Received {} from {}".format(myself, msg1, source))
    nodeState[myself][BLOCKTXN_MSG][RECEIVED] += 1

    block = get_block(myself, tx_requested[0])
    if block is not None:
        # logger.info("Node {} block already built ignore BLOCKTX {}".format(myself, source))
        return

    for tx in tx_requested[1]:
        if tx in nodeState[myself][NODE_TX_ALREADY_REQUESTED]:
            nodeState[myself][NODE_TX_ALREADY_REQUESTED].remove(tx)

    process_block(myself, source, build_cmpctblock(myself, tx_requested))


def TX(myself, source, msg1, tx):
    global nodeState

    # logger.info("Node {} Received {} from {}".format(myself, msg1, source))
    nodeState[myself][TX_MSG][RECEIVED] += 1

    if tx[TX_ID] in nodeState[myself][NODE_TX_ALREADY_REQUESTED]:
        nodeState[myself][NODE_TX_ALREADY_REQUESTED].remove(tx[TX_ID])

    check_tx = get_transaction(myself, tx[TX_ID])
    if check_tx is None:
        nodeState[myself][NODE_MEMPOOL].append(tx)
        nodeState[myself][NODE_vINV_TX_TO_SEND].append(("MSG_TX", tx[TX_ID]))


def next_t_to_gen(myself):
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
        new_block = (block_id, -1, 0, time.time(), myself, tx_array)
    else:
        highest_block = nodeState[myself][NODE_CURRENT_BLOCK]
        new_block = (block_id, highest_block[BLOCK_ID], highest_block[BLOCK_HEIGHT] + 1, time.time(), myself, tx_array)

    # Store the new block
    nodeState[myself][NODE_RECEIVED_BLOCKS].append(new_block)
    nodeState[myself][NODE_CURRENT_BLOCK] = new_block
    block_id += 1
    return new_block


def get_block(myself, block_id):
    for item in reversed(nodeState[myself][NODE_RECEIVED_BLOCKS]):
        if item[0] == block_id:
            return item
    return None


def process_block(myself, source, block):
    global nodeState

    # Check if it's a new block
    if block not in nodeState[myself][NODE_RECEIVED_BLOCKS]:
        update_block(myself, block)
        nodeState[myself][NODE_CURRENT_BLOCK] = block
        next_t_to_gen(myself)
        # Remove tx from MEMPOOL and from vINV_TX_TO_SEND
        update_tx(myself, block)

        # Broadcast new block
        for target in nodeState[myself][NODE_NEIGHBOURHOOD]:
            if target == source or check_availability(myself, target, block[BLOCK_ID]):
                update_availability(myself, source, block)
                continue
            elif check_availability(myself, target, block[BLOCK_PARENT_ID]):
                sim.send(CMPCTBLOCK, target, myself, "CMPCTBLOCK", cmpctblock(block))
                nodeState[myself][CMPCTBLOCK_MSG][SENT] += 1
                nodeState[myself][NODE_BLOCKS_AVAILABILITY].setdefault(target, []).append(block)
            else:
                sim.send(HEADERS, target, myself, "HEADERS", [get_block_header(block)])
                nodeState[myself][HEADERS_MSG][SENT] += 1

    else:
        # This shouldn't happen in a simulated scenario if it happens it also shouldn't affect the results
        logger.info("Node {} Received an unrequested full block from {} INVALID!!!".format(myself, source))


def update_tx(myself, block):
    global nodeState

    for tx in block[BLOCK_TX]:
        if tx in nodeState[myself][NODE_MEMPOOL]:
            nodeState[myself][NODE_MEMPOOL].remove(tx)

            for tx_to_rm in nodeState[myself][NODE_vINV_TX_TO_SEND]:
                if tx[TX_ID] == tx_to_rm[INV_CONTENT_ID]:
                    nodeState[myself][NODE_vINV_TX_TO_SEND].remove(tx_to_rm)
                    break
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
            get_extra_tx_to_send(block[BLOCK_TX])


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


def update_availability(myself, source, block):
    global nodeState

    if source not in nodeState[myself][NODE_BLOCKS_AVAILABILITY].keys():
        nodeState[myself][NODE_BLOCKS_AVAILABILITY].setdefault(source, []).append(block)
        return

    i = len(nodeState[myself][NODE_BLOCKS_AVAILABILITY][source]) - 1
    while i >= 0:
        if nodeState[myself][NODE_BLOCKS_AVAILABILITY][source][i][0] == block[0] and \
                len(nodeState[myself][NODE_BLOCKS_AVAILABILITY][source][i]) > len(block):
            nodeState[myself][NODE_BLOCKS_AVAILABILITY][source][i] = block
            return
        i = i - 1

    nodeState[myself][NODE_BLOCKS_AVAILABILITY].setdefault(source, []).append(block)


def check_availability(myself, target, block_id):
    if target not in nodeState[myself][NODE_BLOCKS_AVAILABILITY].keys():
        return False
    for tpl in reversed(nodeState[myself][NODE_BLOCKS_AVAILABILITY][target]):
        if tpl[0] == block_id:
            return True
    return False


def generate_new_tx(myself):
    global nodeState, tx_id

    new_tx = (tx_id, "This transaction spends " + str(random.randint(0, 100)) + " Bitcoins", myself,
              numpy.random.randint(min_tx_size, max_tx_size))
    nodeState[myself][NODE_MEMPOOL].append(new_tx)
    nodeState[myself][NODE_vINV_TX_TO_SEND].append(("MSG_TX", new_tx[TX_ID]))
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


def get_nb_of_tx_to_gen(size, cycle):
    n = number_of_to_gen_per_cycle/size

    if n != 0:
        tx_gened[cycle] += n
        return n
    else:
        if tx_gened[cycle] < number_of_to_gen_per_cycle:
            if random.random() > 0.5:
                tx_gened[cycle] += 1
                return 1
        return 0


def get_tx_to_block(myself):
    size = 0
    tx_array = []
    for tx in nodeState[myself][NODE_MEMPOOL]:
        if size + tx[TX_SIZE] <= max_block_size:
            size += tx[TX_SIZE]
            tx_array.append(tx)
            nodeState[myself][NODE_MEMPOOL].remove(tx)
        elif size + min_tx_size > max_block_size:
            break
        else:
            continue
    return tx_array


def process_new_headers(myself, source, headers):
    for header in headers:
        header_in = get_block(myself, header[HEADER_ID])
        parent_header_in = get_block(myself, header[HEADER_PARENT_ID])
        if parent_header_in is None and header[HEADER_PARENT_ID] != -1:
            logger.info("Node {} Received a header with a parent that doesn't connect id={} THIS NEEDS TO BE CODED!!".format(myself, header[HEADER_PARENT_ID]))
            headers_to_request = [header[HEADER_PARENT_ID], header[HEADER_ID]]
            sim.send(GETHEADERS, source, myself, "GETHEADERS", headers_to_request)
            nodeState[myself][GETHEADERS_MSG][SENT] += 1
            continue
        elif parent_header_in is not None and not check_availability(myself, source, parent_header_in[BLOCK_ID]):
            update_availability(myself, source, parent_header_in)

        if header_in is None:
            nodeState[myself][NODE_RECEIVED_BLOCKS].append(header)
            update_availability(myself, source, header)
        elif not check_availability(myself, source, header_in[BLOCK_ID]):
            update_availability(myself, source, header_in)


def get_data_to_request(myself, source):
    data_to_request = []
    for block in reversed(nodeState[myself][NODE_RECEIVED_BLOCKS]):
        if len(block) == 4 and check_availability(myself, source, block[BLOCK_ID]) and \
                block[BLOCK_ID] not in nodeState[myself][NODE_BLOCKS_ALREADY_REQUESTED]:
            data_to_request.append(("MSG_BLOCK", block[BLOCK_ID]))
            nodeState[myself][NODE_BLOCKS_ALREADY_REQUESTED].append(block[BLOCK_ID])
        elif len(block) == 4 or len(block) == 6:
            continue
        else:
            # This condition shouldn't happen in a simulated scenario
            raise ValueError("get_data_to_request, else, there are tuples in the NODE_RECEIVED_BLOCKS that do not have an "
                             "expected size. This condition is not coded and shouldn't happen {}".format(block))

    return data_to_request


def wrapup():
    global nodeState
    logger.info("Wrapping up")
    #logger.info(nodeState)

    inv_messages = map(lambda x: nodeState[x][INV_MSG], nodeState)
    getheaders_messages = map(lambda x: nodeState[x][GETHEADERS_MSG], nodeState)
    headers_messages = map(lambda x: nodeState[x][HEADERS_MSG], nodeState)
    getdata_messages = map(lambda x: nodeState[x][GETDATA_MSG], nodeState)
    block_messages = map(lambda x: nodeState[x][BLOCK_MSG], nodeState)
    cmpctblock_messages = map(lambda x: nodeState[x][CMPCTBLOCK_MSG], nodeState)
    getblocktx_messages = map(lambda x: nodeState[x][GETBLOCKTXN_MSG], nodeState)
    blocktx_messages = map(lambda x: nodeState[x][BLOCKTXN_MSG], nodeState)
    tx_messages = map(lambda x: nodeState[x][TX_MSG], nodeState)

    sum_received_blocks = map(lambda x: nodeState[x][NODE_RECEIVED_BLOCKS], nodeState)
    receivedBlocks = map(lambda x: map(lambda y: (sum_received_blocks[x][y][0], sum_received_blocks[x][y][1],
                                                  sum_received_blocks[x][y][2], sum_received_blocks[x][y][3]),
                                       xrange(len(sum_received_blocks[x]))), nodeState)
    sum_received_blocks = map(lambda x: map(lambda y: sum_received_blocks[x][y][0], xrange(len(sum_received_blocks[x]))), nodeState)

    # dump data into gnuplot format
    utils.dump_as_gnu_plot([inv_messages, getheaders_messages, headers_messages, getdata_messages, block_messages,
                         cmpctblock_messages, getblocktx_messages, blocktx_messages, tx_messages, sum_received_blocks,
                         receivedBlocks],
                        dumpPath + '/messages-' + str(runId) + '.gpData',
                        ['inv getheaders headers getdata block cmpctblock getblocktx blocktx tx'
                         '           sum_received_blocks                    receivedBlocks'])

    utils.compute_average_of_message("getblocktx", getblocktx_messages, dumpPath)

 #   with open(dumpPath + '/dumps-' + str(runId) + '.obj', 'w') as f:
  #      cPickle.dump(receivedMessages, f)
   #     cPickle.dump(sentMessages, f)


def createNode(neighbourhood):
    return [0, [0, 0], [0, 0], [0, 0], [0, 0], [0, 0], [0, 0], [0, 0], [0, 0], [0, 0], None, neighbourhood,
            [], [], {}, [], [], [], [], -1]


def configure(config):
    global nbNodes, nbCycles, prob_generating_block, nodeState, nodeCycle, block_id, max_block_number, tx_id,\
        number_of_to_gen_per_cycle, tx_gened, max_block_size, min_tx_size, max_tx_size, values

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
    number_of_to_gen_per_cycle = config['NUMB_TX_PER_CYCLE']
    nodeDrift = int(nodeCycle * float(config['NODE_DRIFT']))

    max_block_size = int(config['MAX_BLOCK_SIZE'])
    min_tx_size = int(config['MIN_TX_SIZE'])
    max_tx_size = int(config['MAX_TX_SIZE'])


    latencyTablePath = config['LATENCY_TABLE']
    latencyValue = None

    tx_gened = [0] * nbCycles

    values = []
    i = 0
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
