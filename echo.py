# Sample simulator demo
# Miguel Matos - miguel.marques.matos@tecnico.ulisboa.pt
# (c) 2012-2018
from __future__ import division

import ast
import csv
import gc

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
CURRENT_CYCLE, NODE_NEIGHBOURHOOD, MSGS = 0, 1, 2

# Node neighbour structure
INBOUND, PING_STRC, ADDR_STRC = 0, 1, 2

# Addr structure
SENT_ADDR, NEXT_ADDR_SEND, NEXT_LOCAL_ADDR_SEND, ADDR_TO_SEND, ADDR_KNOWN = 0, 1, 2, 3, 4

AVG_LOCAL_ADDRESS_BROADCAST_INTERVAL = 24 * 60 * 60

AVG_ADDRESS_BROADCAST_INTERVAL = 30

MAX_ADDR_MSG_SIZE = 1000

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

    for node in nodeState[myself][NODE_NEIGHBOURHOOD]:
        cnode = nodeState[myself][NODE_NEIGHBOURHOOD][node]
        send_messages(myself, node, cnode)

    nodeState[myself][CURRENT_CYCLE] += 1
    # schedule next execution
    if nodeState[myself][CURRENT_CYCLE] < nb_cycles:
        sim.schedulleExecution(CYCLE, myself)


def send_messages(myself, target, pto):
    # TODO check if we are successfully connected
    send_ping(myself, target, pto)
    send_reject_and_check_if_banned(myself, target, pto)
    address_refresh_broadcast(myself, target, pto)
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
    return


def address_refresh_broadcast(myself, pto):
    if pto[ADDR_STRC][NEXT_LOCAL_ADDR_SEND] < nodeState[myself][CURRENT_CYCLE]:
        push_address(pto, myself)
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


def VERSION(myself, source):
    global nodeState

    if should_log(myself):
        nodeState[myself][MSGS][VERSION_MSG] += 1
    new_connection(myself, source)


def VERACK(myself, source):
    global nodeState

    # TODO prevent nodes from starting communications with this message in case that could happen

    if should_log(myself):
        nodeState[myself][MSGS][VERACK_MSG] += 1


def GETADDR(myself, source):
    global nodeState

    # TODO prevent nodes from starting communications with this message in case that could happen

    if should_log(myself):
        nodeState[myself][MSGS][GET_ADDR_MSG] += 1

    pto = nodeState[myself][NODE_NEIGHBOURHOOD][source]
    if not pto[INBOUND]:
        return

    if pto[ADDR_STRC][SENT_ADDR]:
        return

    pto[ADDR_STRC][SENT_ADDR] = True

    pto[ADDR_STRC][ADDR_TO_SEND] = []
    addr_to_send = nodeState[myself][NODE_NEIGHBOURHOOD].keys()
    for addr in addr_to_send:
        push_address(pto, addr)


def ADDR(myself, source):
    global nodeState

    # TODO prevent nodes from starting communications with this message in case that could happen

    if should_log(myself):
        nodeState[myself][MSGS][ADDR_MSG] += 1


def PING(myself, source, nonce):
    global nodeState

    # TODO prevent nodes from starting communications with this message in case that could happen

    if should_log(myself):
        nodeState[myself][MSGS][PING_MSG] += 1

    sim.send(PONG, source, myself, nonce)


def PONG(myself, source, nonce):
    global nodeState

    # TODO prevent nodes from starting communications with this message in case that could happen
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


# Addr functions
def add_addr(pto, addr):
    if addr not in pto[ADDR_STRC][ADDR_KNOWN]:
        pto[ADDR_STRC][ADDR_KNOWN].append(addr)


def has_addr(pto, addr):
    if addr in pto[ADDR_STRC][ADDR_KNOWN]:
        return True
    return False


def push_address(pto, addr):
    if not has_addr(pto, addr):
        if len(pto[ADDR_STRC][ADDR_TO_SEND]) >= MAX_ADDR_MSG_SIZE:
            i = random.randint(len(pto[ADDR_STRC][ADDR_TO_SEND]))
            pto[ADDR_STRC][ADDR_TO_SEND][i] = addr
        else:
            pto[ADDR_STRC][ADDR_TO_SEND].append(addr)


def should_log(myself):
    if (expert_log and INTERVAL < nodeState[myself][CURRENT_CYCLE] < nb_cycles - INTERVAL) or not expert_log:
        return True
    return False


# Broadcast functions
def poisson_next_send(now, avg_inc):
        return now + avg_inc * random.randrange(1, 5)


def new_connection(myself, source):
    global nodeState

    if len(nodeState[myself][NODE_NEIGHBOURHOOD]) > 125:
        raise ValueError("Number of connections in one node exceed the maximum allowed")

    if source in nodeState[myself][NODE_NEIGHBOURHOOD]:
        return
    else:
        nodeState[myself][NODE_NEIGHBOURHOOD][source] = create_neighbour(True)


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
    current_cycle = 0
    msgs = [0, 0, 0, 0, 0]
    neighbourhood_dic = defaultdict()
    for neighbour in neighbourhood:
        neighbourhood_dic[neighbour] = create_neighbour(False)

    return [current_cycle, neighbourhood_dic, msgs]


def create_neighbour(inbound):
    ping = [0, 0, 0, 0]
    addr = [False, 0, 0, [], []]
    return [inbound, ping, addr]


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
