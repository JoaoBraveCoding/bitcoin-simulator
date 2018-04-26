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

from sim import sim
import utils

LOG_TO_FILE = False

BLOCK_ID, BLOCK_PARENT_ID, BLOCK_HEIGHT, BLOCK_TIMESTAMP, BLOCK_GEN_NODE, BLOCK_TX = 0, 1, 2, 3, 4, 5
TX_ID, TX_CONTENT, TX_GEN_NODE = 0, 1, 2
INV_TYPE, INV_CONTENT_ID = 0, 1
HEADER_ID, HEADER_PARENT_ID, HEADER_TIMESTAMP, HEADER_GEN_NODE = 0, 1, 2, 3

CURRENT_CYCLE, MSGS_RECEIVED, MSGS_SENT, NODE_CURRENT_BLOCK, NODE_NEIGHBOURHOOD, NODE_RECEIVED_BLOCKS, \
    NODE_PARTIAL_BLOCKS, NODE_BLOCKS_AVAILABILITY, NODE_MEMPOOL, NODE_vINV_TX_TO_SEND = 0, 1, 2, 3, 4, 5, 6, 7, 9, 10


def init():
    global nodeState

    # schedule execution for all nodes
    for nodeId in nodeState:
        sim.schedulleExecution(CYCLE, nodeId)

    # other things such as periodic measurements can also be scheduled
    # to schedule periodic measurements use the following
    # for c in range(nbCycles * 10):
    #    sim.scheduleExecutionFixed(MEASURE_X, nodeCycle * (c + 1))


def CYCLE(myself):
    global nodeState, block_id, tx_id

    # with churn the node might be gone
    if myself not in nodeState:
        return

    # show progress for one node
    if myself == 0:
        logger.info('node {} cycle {}'.format(myself, nodeState[myself][CURRENT_CYCLE]))

    nodeState[myself][CURRENT_CYCLE] += 1

    # schedule next execution
    if nodeState[myself][CURRENT_CYCLE] < nbCycles:
        sim.schedulleExecution(CYCLE, myself)

    # If a node can generate transactions
    if random.random() <= prob_generating_trans:
        generate_new_tx(myself)

    # If the node can generate a block
    if random.random() <= prob_generating_block and (max_block_number == 0 or block_id < max_block_number):
        new_block = generate_new_block(myself)

        # Check if can send as cmpct or send through inv
        for target in nodeState[myself][NODE_NEIGHBOURHOOD]:
            if nodeState[myself][NODE_CURRENT_BLOCK] is not None and check_availability(myself, target, NODE_CURRENT_BLOCK):
                sim.send(CMPCTBLOCK, target, myself, "CMPCTBLOCK", cmpctblock(new_block))
                nodeState[myself][NODE_BLOCKS_AVAILABILITY].setdefault(target, []).append(new_block)

            else:
                vInv = [("MSG_BLOCK", new_block[BLOCK_ID])]
                sim.send(INV, target, myself, "INV", vInv)
            nodeState[myself][MSGS_SENT] += 1

    # Send new transactions either created or received
    if not nodeState[myself][NODE_vINV_TX_TO_SEND]:
        for target in nodeState[myself][NODE_NEIGHBOURHOOD]:
            sim.send(INV, target, myself, "INV", nodeState[myself][NODE_vINV_TX_TO_SEND])
            nodeState[myself][MSGS_SENT] += 1
        nodeState[myself][NODE_vINV_TX_TO_SEND] = []


def INV(myself, source, msg1, vInv):
    global nodeState

    logger.info("Node {} Received {} from {}".format(myself, msg1, source))
    nodeState[myself][MSGS_RECEIVED] += 1

    ask_for = []
    headers_to_request = []
    for inv in vInv:
        if inv[INV_TYPE] == "MSG_TX":
            tx = get_transaction(myself, inv[INV_CONTENT_ID])
            if tx is None:
                ask_for.append(inv)

        elif inv[INV_TYPE] == "MSG_BLOCK":
            block = get_block(myself, inv[INV_CONTENT_ID])
            if block is None:
                headers_to_request.append(inv[INV_CONTENT_ID])
                update_availability(myself, source, (inv[INV_CONTENT_ID],))
            else:
                update_availability(myself, source, block)
        else:
            logger.info("Node {} Received INV from {} INVALID INV!!!".format(myself, source))
            raise ValueError('INV, else, node received invalid inv type this condition is not coded')

    if ask_for:
        sim.send(GETDATA, source, myself, "GETDATA", ask_for)

    if headers_to_request:
        sim.send(GETHEADERS, source, myself, "GETHEADERS", headers_to_request)


def GETHEADERS(myself, source, msg1, get_headers):
    global nodeState

    logger.info("Node {} Received {} from {}".format(myself, msg1, source))
    nodeState[myself][MSGS_RECEIVED] += 1

    headers_to_send = []
    for id in get_headers:
        block = get_block(myself, id)
        if block is not None:
            headers_to_send.append(get_block_header(block))
        else:
            logger.info("Node {} Received header from {} INVALID ID in header!!!".format(myself, source))
            raise ValueError('GETHEADERS, else, node received invalid headerID')

    sim.send(HEADERS, source, myself, "HEADERS", headers_to_send)


def HEADERS(myself, source, msg1, headers):
    global nodeState

    logger.info("Node {} Received {} from {}".format(myself, msg1, source))
    nodeState[myself][MSGS_RECEIVED] += 1

    process_new_headers(myself, source, headers)
    data_to_request = get_data_to_request(myself, source)
    if len(data_to_request) <= 16:
        # If is a new block in the main chain try and direct fetch
        sim.send(GETDATA, source, myself, "GETDATA", data_to_request)
    else:
        # Else rely on other means of download
        # TODO Fix this case still don't know how it's done
        logger.info("Node {} received more than 16 headers from {} INVALID!!!".format(myself, source))
        raise ValueError('HEADERS, else, this condition is not coded')


def GETDATA(myself, source, msg1, requesting_data):
    global nodeState

    logger.info("Node {} Received {} from {}".format(myself, msg1, source))
    nodeState[myself][MSGS_RECEIVED] += 1

    for inv in requesting_data:
        if inv[INV_TYPE] == "MSG_TX":
            tx = get_transaction(myself, inv[INV_CONTENT_ID])
            if tx is not None:
                sim.send(TX, source, myself, "TX", tx)
            else:
                # TODO Fix me either way this shouldn't happen
                logger.info(
                    "Node {} Received more invalid inv_id for a transation in GETDATA from {} INVALID!!!".format(myself, source))
                raise ValueError('GETDATA, MSG_TX else, this condition is not coded and shouldn\'t happen')

        elif inv[INV_TYPE] == "MSG_BLOCK":
            block = get_block(myself, inv[INV_CONTENT_ID])
            if block is not None:
                sim.send(BLOCK, source, myself, "BLOCK", block)
                update_availability(myself, source, block)
            else:
                # TODO Fix me either way this shouldn't happen it
                logger.info(
                    "Node {} Received {} from {} with invalid block_id in GETDATA INVALID REQUEST".format(myself, msg1, source))
                raise ValueError('GETDATA, MSG_BLOCK else, this condition is not coded and shouldn\'t happen')

        else:
            # TODO Fix me either way this shouldn't happen
            logger.info("Node {} Received more invalid inv type in GETDATA from {} INVALID!!!".format(myself, source))
            raise ValueError('GETDATA, else, this condition is not coded and shouldn\'t happen')


def BLOCK(myself, source, msg1, block):
    global nodeState

    logger.info("Node {} Received {} from {}".format(myself, msg1, source))
    nodeState[myself][MSGS_RECEIVED] += 1

    process_block(myself, source, block)


def CMPCTBLOCK(myself, source, msg1, cmpctblock):
    global nodeState

    logger.info("Node {} Received {} from {}".format(myself, msg1, source))
    nodeState[myself][MSGS_RECEIVED] += 1

    # Check if we have all tx
    tx_to_request = []
    tx_in_block = []
    for mini_tx in cmpctblock[BLOCK_TX]:
        tx = get_transaction(myself, mini_tx)
        if tx is None:
            tx_to_request.append(mini_tx)
        else:
            tx_in_block.append(tx)

    if tx_to_request:
        sim.send(GETBLOCKTXN, source, myself, "GETBLOCKTXN", (cmpctblock[BLOCK_ID], tx_to_request))
        if cmpctblock not in nodeState[myself][NODE_PARTIAL_BLOCKS]:
            nodeState[myself][NODE_PARTIAL_BLOCKS].append(cmpctblock)
        return

    block = (cmpctblock[BLOCK_ID], cmpctblock[BLOCK_PARENT_ID], cmpctblock[BLOCK_HEIGHT], cmpctblock[BLOCK_TIMESTAMP],
             cmpctblock[BLOCK_GEN_NODE], tx_in_block)

    process_block(myself, source, block)


def GETBLOCKTXN(myself, source, msg1, tx_request):
    global nodeState

    logger.info("Node {} Received {} from {}".format(myself, msg1, source))
    nodeState[myself][MSGS_RECEIVED] += 1

    block = get_block(myself, tx_request[0])
    tx_to_send = []
    for tx in block[BLOCK_TX]:
        if tx[TX_ID] in tx_request[1]:
            tx_to_send.append(tx)

    if len(tx_to_send) != len(tx_request[1]):
        logger.info("Node {} Received invalid tx_id in GETBLOCKTXN from {} INVALID!!!".format(myself, source))
        raise ValueError('GETBLOCKTXN, else, this condition is not coded invalid tx_id')

    if tx_to_send:
        sim.send(BLOCKTXN, source, myself, "BLOCKTXN", (tx_request[0], tx_to_send))


def BLOCKTXN(myself, source, msg1, tx_requested):
    global nodeState

    logger.info("Node {} Received {} from {}".format(myself, msg1, source))
    nodeState[myself][MSGS_RECEIVED] += 1

    block = get_block(myself, tx_requested[0])
    if block is not None:
        logger.info("Node {} block already built ignore BLOCKTX {}".format(myself, source))
        return

    for tx in tx_requested[1]:
        if tx not in nodeState[myself][NODE_MEMPOOL]:
            nodeState[myself][NODE_MEMPOOL].append(tx)
        else:
            logger.info("Node {} Received tx in BLOCKTXN that already had from {} INVALID!!!".format(myself, source))
            # raise ValueError('BLOCKTXN, else, this condition is not coded, already known tx')

    process_block(myself, source, build_cmpctblock(myself, tx_requested[0]))


def TX(myself, source, msg1, tx):
    global nodeState

    logger.info("Node {} Received {} from {}".format(myself, msg1, source))
    nodeState[myself][MSGS_RECEIVED] += 1

    check_tx = get_transaction(myself, tx[TX_ID])
    if check_tx is None:
        nodeState[myself][NODE_MEMPOOL].append(tx)
        nodeState[myself][NODE_vINV_TX_TO_SEND].append(tx)


def generate_new_block(myself):
    global nodeState, block_id

    # First block or
    # Not first block which means getting highest block to be the parent
    if nodeState[myself][NODE_CURRENT_BLOCK] is None:
        new_block = (block_id, -1, 0, time.time(), myself, nodeState[myself][NODE_MEMPOOL])
        nodeState[myself][NODE_MEMPOOL] = []
    else:
        highest_block = nodeState[myself][NODE_CURRENT_BLOCK]
        new_block = (block_id, highest_block[BLOCK_ID], highest_block[BLOCK_HEIGHT] + 1, time.time(), myself,
                     nodeState[myself][NODE_MEMPOOL])
        nodeState[myself][NODE_MEMPOOL] = []

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
    # TODO check if cmpt block is broadcast only when it's higher
    # Check if it's a new block
    if block not in nodeState[myself][NODE_RECEIVED_BLOCKS]:
        update_block(myself, block)
        if nodeState[myself][NODE_CURRENT_BLOCK] is None or \
                block[BLOCK_PARENT_ID] == nodeState[myself][NODE_CURRENT_BLOCK][BLOCK_ID]:
            nodeState[myself][NODE_CURRENT_BLOCK] = block
        elif block[BLOCK_HEIGHT] > nodeState[myself][NODE_CURRENT_BLOCK][BLOCK_HEIGHT]:
            # TODO implement re-branch
            nodeState[myself][NODE_CURRENT_BLOCK] = block

        # Remove tx from MEMPOOL and from vINV_TX_TO_SEND
        for tx in block[BLOCK_TX]:
            if tx in nodeState[myself][NODE_MEMPOOL]:
                nodeState[myself][NODE_MEMPOOL].remove(tx)

                for tx_to_rm in nodeState[myself][NODE_vINV_TX_TO_SEND]:
                    if tx[TX_ID] == tx_to_rm[INV_CONTENT_ID]:
                        nodeState[myself][NODE_vINV_TX_TO_SEND].remove(tx_to_rm)
                        break

        # Update or create block availability for neighbourhood
        update_availability(myself, source, block)

        # Broadcast new block
        for target in nodeState[myself][NODE_NEIGHBOURHOOD]:
            if target == source or check_availability(myself, target, block[BLOCK_ID]):
                continue
            elif check_availability(myself, target, block[BLOCK_PARENT_ID]):
                sim.send(CMPCTBLOCK, target, myself, "CMPCTBLOCK", cmpctblock(block))
                nodeState[myself][NODE_BLOCKS_AVAILABILITY].setdefault(target, []).append(block)
            else:
                sim.send(HEADERS, target, myself, "HEADERS", [get_block_header(block)])
            nodeState[myself][MSGS_SENT] += 1

    else:
        # TODO Fix me either way this shouldn't happen IMPORTANT
        logger.info("Node {} Received an unrequested full block from {} INVALID!!!".format(myself, source))
        # raise ValueError('BLOCK, else, this condition is not coded and shouldn\'t happen')


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
    return block[BLOCK_ID], block[BLOCK_PARENT_ID], block[BLOCK_HEIGHT], block[BLOCK_TIMESTAMP], block[BLOCK_GEN_NODE], cmpct_tx


def get_cmpctblock(myself, block_id):
    for partial_block in nodeState[myself][NODE_PARTIAL_BLOCKS]:
        if partial_block[BLOCK_ID] == block_id:
            return partial_block

    return None


def build_cmpctblock(myself, block_id):
    cmpctblock = get_cmpctblock(myself, block_id)

    if cmpctblock is None:
        raise ValueError('build_cmpctblock,  if cmpctblock is None, this condition is not coded, cmpctblock_id not in partialblocks')

    tx_in_block = []
    for tx_id in cmpctblock[BLOCK_TX]:
        tx = get_transaction(myself, tx_id)
        if tx is not None:
            tx_in_block.append(tx)
        else:
            raise ValueError('build_cmpctblock, else, this condition is not coded, tx in block but not in mempool')

    return cmpctblock[BLOCK_ID], cmpctblock[BLOCK_PARENT_ID], cmpctblock[BLOCK_HEIGHT], cmpctblock[BLOCK_TIMESTAMP], \
           cmpctblock[BLOCK_GEN_NODE], tx_in_block


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

    new_tx = (tx_id, "This transaction spends " + str(random.randint(0, 100)) + " Bitcoins", myself)
    nodeState[myself][NODE_MEMPOOL].append(new_tx)
    nodeState[myself][NODE_vINV_TX_TO_SEND].append(("MSG_TX", new_tx))
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


def process_new_headers(myself, source, headers):
    for header in headers:
        header_in = get_block(myself, header[HEADER_ID])
        parent_header_in = get_block(myself, header[HEADER_PARENT_ID])
        if parent_header_in is None and header[HEADER_PARENT_ID] != -1:
            # TODO Fix me
            logger.info("Node {} Received a header with a parent that doesn't connect id={} THIS NEEDS TO BE CODED!!".format(myself, header[HEADER_PARENT_ID]))
            raise ValueError('process_new_headers, if parent_header_in is None, this condition is not coded')
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
        if len(block) == 6:
            continue
        elif len(block) == 4 and check_availability(myself, source, block[BLOCK_ID]):
            #TODO IMPORTANT add list of requests already made
            data_to_request.append(("MSG_BLOCK", block[BLOCK_ID]))
        elif len(block) == 4:
            continue
        else:
            # TODO this shouldn't happen
            raise ValueError("get_data_to_request, else, this condition is not coded and shouldn't happen")

    return data_to_request


def wrapup():
    global nodeState
    logger.info("Wrapping up")
    #logger.info(nodeState)

    receivedMessages = map(lambda x: nodeState[x][MSGS_RECEIVED], nodeState)
    sentMessages = map(lambda x: nodeState[x][MSGS_SENT], nodeState)
    sum_received_blocks = map(lambda x: nodeState[x][NODE_RECEIVED_BLOCKS], nodeState)
    receivedBlocks = map(lambda x: map(lambda y: (sum_received_blocks[x][y][0], sum_received_blocks[x][y][1],
                                                  sum_received_blocks[x][y][2], sum_received_blocks[x][y][3]) ,
                                       xrange(len(sum_received_blocks[x]))), nodeState)
    sum_received_blocks = map(lambda x: map(lambda y: sum_received_blocks[x][y][0], xrange(len(sum_received_blocks[x]))), nodeState)

    # gather some stats, see utils for more functions
    logger.info("receivedMessages {}".format(receivedMessages))
    logger.info("receivedMessages min: {}, max: {}, total: {}".format(min(receivedMessages), max(receivedMessages),
                                                                      sum(receivedMessages)))
    logger.info("sentMessages {}".format(sentMessages))
    logger.info(
        "sentMessages min: {}, max: {}, total: {}".format(min(sentMessages), max(sentMessages), sum(receivedMessages)))

    # dump data into gnuplot format
    utils.dumpAsGnuplot([receivedMessages, sentMessages, sum_received_blocks, receivedBlocks],
                        dumpPath + '/messages-' + str(runId) + '.gpData',
                        ['receivedMessages sentMessages sum_received_blocks receivedBlocks'])

    # dump data for later processing
    with open(dumpPath + '/dumps-' + str(runId) + '.obj', 'w') as f:
        cPickle.dump(receivedMessages, f)
        cPickle.dump(sentMessages, f)


def createNode(neighbourhood):
    return [0, 0, 0, None, neighbourhood, [], [], {}, [], [], []]


def configure(config):
    global nbNodes, nbCycles, prob_generating_block, nodeState, nodeCycle, block_id, max_block_number, tx_id, prob_generating_trans

    IS_CHURN = config.get('CHURN', False)
    if IS_CHURN:
        CHURN_RATE = config.get('CHURN_RATE', 0.)
    MESSAGE_LOSS = float(config.get('MESSASE_LOSS', 0))
    if MESSAGE_LOSS > 0:
        sim.setMessageLoss(MESSAGE_LOSS)

    nbNodes = config['nbNodes']
    nbCycles = config['nbCycles']
    nodeCycle = int(config['NODE_CYCLE'])
    neighbourhood_size = int(config['NEIGHBOURHOOD_SIZE'])
    prob_generating_block = config['PROB_GEN_BLOCK']
    max_block_number = int(config['MAX_NUMBER_OF_BLOCKS'])
    prob_generating_trans = config['PROB_GEN_TRANS']
    nodeDrift = int(nodeCycle * float(config['NODE_DRIFT']))

    latencyTablePath = config['LATENCY_TABLE']
    latencyValue = None
    try:
        with open(latencyTablePath, 'r') as f:
            latencyTable = cPickle.load(f)
    except:
        latencyTable = None
        latencyValue = int(latencyTablePath)
        logger.warn('Using constant latency value: {}'.format(latencyValue))

    latencyTable = utils.checkLatencyNodes(latencyTable, nbNodes, latencyValue)
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
