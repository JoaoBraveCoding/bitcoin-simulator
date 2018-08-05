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
import numpy
from sim import sim
import utils
from sortedList import SortedCollection

# Messages structures

# Node structure
CURRENT_CYCLE, NODE_NEIGHBOURHOOD, MSGS = 0, 1, 2

# Counter structure
VERSION_MSG, VERACK_MSG, ADDR_MSG, PING_MSG, PONG_MSG = 0, 1, 2, 3, 4

SENT, RECEIVED = 0, 1

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
        output.write('{} id: {} cycle: {}\n'.format(value.strftime('%Y-%m-%d %H:%M:%S'), nodeState[myself][CURRENT_CYCLE], runId))
        output.flush()
        #print('{} id: {} cycle: {}\n'.format(value.strftime('%Y-%m-%d %H:%M:%S'), nodeState[myself][CURRENT_CYCLE], runId))

    nodeState[myself][CURRENT_CYCLE] += 1
    # schedule next execution
    if nodeState[myself][CURRENT_CYCLE] < nb_cycles:
        sim.schedulleExecution(CYCLE, myself)


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


def ADDR(myself, source):
    global nodeState

    # TODO prevent nodes from starting communications with this message in case that could happen

    if should_log(myself):
        nodeState[myself][MSGS][ADDR_MSG] += 1


def PING(myself, source):
    global nodeState

    # TODO prevent nodes from starting communications with this message in case that could happen

    if should_log(myself):
        nodeState[myself][MSGS][PING_MSG] += 1

    smth = None
    sim.send(PONG, source, myself, smth)


def PONG(myself, source):
    global nodeState

    # TODO prevent nodes from starting communications with this message in case that could happen

    if should_log(myself):
        nodeState[myself][MSGS][PONG_MSG] += 1


def should_log(myself):
    if (expert_log and INTERVAL < nodeState[myself][CURRENT_CYCLE] < nb_cycles - INTERVAL) or not expert_log:
        return True
    return False


def new_connection(myself, source):
    global nodeState

    if len(nodeState[myself][NODE_NEIGHBOURHOOD]) > 125:
        raise ValueError("Number of connections in one node exceed the maximum allowed")

    if source in nodeState[myself][NODE_NEIGHBOURHOOD]:
        return
    else:
        nodeState[myself][NODE_NEIGHBOURHOOD].append(source)


# --------------------------------------
# Start up functions
def create_network(create_new, neighbourhood_size, filename=""):

    first_time = not os.path.exists("networks/")
    network_first_time = not os.path.exists("networks/" + filename)

    if first_time:
        os.makedirs("networks/")

    if network_first_time or create_new:
        create_nodes(neighbourhood_size)
        if save_network_connections:
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

    return [current_cycle, neighbourhood, msgs]


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


def create_bad_node():
    global bad_nodes

    bad_nodes = random.sample(xrange(nb_nodes), int((number_of_bad_nodes/100) * nb_nodes))


def configure(config):
    global nb_nodes, nb_cycles, nodeState, node_cycle, block_id, tx_id, \
        number_of_tx_to_gen_per_cycle, max_block_size, min_tx_size, max_tx_size, values, nodes_to_gen_tx, miners, \
        top_nodes_size, hop_based_broadcast, number_of_miners, extra_replicas, blocks_created, blocks_mined_by_randoms, \
        total_blocks_mined_by_randoms, highest_block, random_nodes_size, tx_created, tx_array, expert_log, bad_nodes, \
        number_of_bad_nodes, tx_commit, tx_created_after_last_block, timer_solution

    node_cycle = int(config['NODE_CYCLE'])

    nb_nodes = config['NUMBER_OF_NODES']
    neighbourhood_size = int(config['NEIGHBOURHOOD_SIZE'])
    if top_nodes != -1:
        if top_nodes == 0:
            hop_based_broadcast = False
        else:
            hop_based_broadcast = True
            if not timer_solution:
                timer_solution = bool(config['TIMER_SOLUTION'])
        top_nodes_size = top_nodes
        if random_nodes != -1:
            random_nodes_size = random_nodes
        else:
            random_nodes_size = top_nodes
    else:
        top_nodes_size = int(config['TOP_NODES_SIZE'])
        random_nodes_size = int(config['RANDOM_NODES_SIZE'])
        hop_based_broadcast = bool(config['HOP_BASED_BROADCAST'])
        if not timer_solution:
            timer_solution = bool(config['TIMER_SOLUTION'])

    if number_of_bad_nodes == 0:
        number_of_bad_nodes = int(config['NUMBER_OF_BAD_NODES'])

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
    if nb_cycles <= INTERVAL:
        raise ValueError("You have to complete more than {} cycles".format(INTERVAL))

    block_id = 0
    blocks_created = []
    highest_block = -1
    tx_id = 0
    tx_created = []
    tx_commit = []
    tx_created_after_last_block = []

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
            nodes_to_gen_tx.append(random.sample(xrange(nb_nodes), number_of_tx_to_gen_per_cycle))

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
        if (expert_log and INTERVAL < block[BLOCK_TIMESTAMP] < nb_cycles - INTERVAL) or not expert_log:
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


def get_nb_tx_added_to_blocks():
    counter = 0
    for tx in tx_commit:
        if tx[COMMITED]:
            counter += 1
    return counter


def get_nb_of_tx_gened():
    tx_in_miners = []
    for myself in miners:
        for tx in nodeState[myself][NODE_MEMPOOL]:
            if tx not in tx_created_after_last_block and not tx_commit[tx][COMMITED] and tx not in tx_in_miners:
                tx_in_miners.append(tx)

    return tx_id - len(tx_created_after_last_block) - len(tx_in_miners)


def get_avg_time_commited():
    counter = 0
    sum = 0
    for tx in tx_commit:
        if tx[COMMITED]:
            sum += tx[TIME_COMMITED]
            counter += 1
    return sum/counter


def wrapup():
    global nodeState

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
    if sum_all_inv == 0:
        avg_duplicated_inv = 0
        avg_entries_per_inv = 0
    else:
        avg_duplicated_inv = sum_all_inv/sum_relevant_inv
        avg_entries_per_inv = sum_all_inv/sum_received_invs
    # ---------
    if sum_all_getdata == 0:
        avg_entries_per_getdata = 0
    else:
        avg_entries_per_getdata = sum_all_getdata/sum_received_getdata

    nb_tx_added_to_blocks = get_nb_tx_added_to_blocks()
    nb_of_tx_gened = get_nb_of_tx_gened()
    avg_time_commited = get_avg_time_commited()

    data = []
    for tx in tx_commit:
        if tx[COMMITED]:
            data.append(tx[TIME_COMMITED])

    time_commited_CDF = utils.percentiles(data, percs=range(101), paired=False)

    utils.dump_as_gnu_plot([time_commited_CDF], dumpPath + '/time_commited_CDF-' + str(runId) + '.gpData', ['time_commited'])

    first_time = not os.path.isfile('out/{}.csv'.format(results_name))
    if first_time:
        csv_file_to_write = open('out/results.csv', 'w')
        backup = open('backup.txt', 'w')
        spam_writer = csv.writer(csv_file_to_write, delimiter=',', quotechar='\'', quoting=csv.QUOTE_MINIMAL)
        spam_writer.writerow(["Number of nodes", "Number of cycles", "Number of miners", "Extra miners"])
        spam_writer.writerow([nb_nodes, nb_cycles, number_of_miners, extra_replicas])
        spam_writer.writerow(["Top nodes size", "Random nodes size", "Early push", "Bad nodes", "Timer solution",
                              "Avg inv", "Avg entries per inv",
                              "Avg getData", "Avg entries per getData", "Avg Tx", "Avg getBlockTX",
                              "Avg missing tx", "Avg numb of tx per block", "% of duplicates inv", "Avg total sent messages",
                              "Total tx created", "Total tx added to blocks", "Avg commit time", "Total number of branches",
                              "Total blocks created", "Hops distribution"])
    else:
        csv_file_to_write = open('out/results.csv', 'a')
        backup = open('backup.txt', 'a')
        spam_writer = csv.writer(csv_file_to_write, delimiter=',', quotechar='\'', quoting=csv.QUOTE_MINIMAL)

    if not hop_based_broadcast:
        spam_writer.writerow(["False", "False", early_push, number_of_bad_nodes, timer_solution, sum_inv / nb_nodes,
                              avg_entries_per_inv, sum_getData / nb_nodes,
                              avg_entries_per_getdata, sum_tx / nb_nodes, sum_getBlockTX / nb_nodes,
                              sum_missingTX / nb_nodes, avg_tx_per_block, avg_duplicated_inv,
                              avg_total_sent_msg, nb_of_tx_gened, nb_tx_added_to_blocks, avg_time_commited,  nb_forks, block_id,
                              ''.join(str(e) + " " for e in hops_distribution)])
        backup.write("{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}"
                     .format("False", "False", early_push, number_of_bad_nodes, timer_solution, sum_inv / nb_nodes,
                             avg_entries_per_inv, sum_getData / nb_nodes,
                              avg_entries_per_getdata, sum_tx / nb_nodes, sum_getBlockTX / nb_nodes,
                              sum_missingTX / nb_nodes, avg_tx_per_block, avg_duplicated_inv,
                              avg_total_sent_msg, nb_of_tx_gened, nb_tx_added_to_blocks, avg_time_commited,  nb_forks, block_id,
                              ''.join(str(e) + " " for e in hops_distribution)))
    else:
        spam_writer.writerow([top_nodes_size, random_nodes_size, early_push, number_of_bad_nodes, timer_solution,
                              sum_inv / nb_nodes, avg_entries_per_inv,
                              sum_getData / nb_nodes, avg_entries_per_getdata, sum_tx / nb_nodes,
                              sum_getBlockTX / nb_nodes, sum_missingTX / nb_nodes, avg_tx_per_block,
                              avg_duplicated_inv, avg_total_sent_msg, nb_of_tx_gened, nb_tx_added_to_blocks, avg_time_commited,
                              nb_forks, block_id,
                              ''.join(str(e) + " " for e in hops_distribution)])
        backup.write("{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}"
                     .format(top_nodes_size, random_nodes_size, early_push, number_of_bad_nodes, timer_solution,
                              sum_inv / nb_nodes, avg_entries_per_inv,
                              sum_getData / nb_nodes, avg_entries_per_getdata, sum_tx / nb_nodes,
                              sum_getBlockTX / nb_nodes, sum_missingTX / nb_nodes, avg_tx_per_block,
                              avg_duplicated_inv, avg_total_sent_msg, nb_of_tx_gened, nb_tx_added_to_blocks, avg_time_commited,
                              nb_forks, block_id,
                              ''.join(str(e) + " " for e in hops_distribution)))
    backup.close()
    csv_file_to_write.flush()
    csv_file_to_write.close()
    print(tx_id)
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

    top_nodes = -1
    random_nodes = -1
    create_new = True
    early_push = False
    save_network_connections = False
    file_name = ""
    results_name = "results"
    number_of_bad_nodes = 0
    timer_solution = False
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
            elif sys.argv[i] == "-bn":
                number_of_bad_nodes = int(sys.argv[i+1])
            elif sys.argv[i] == "-ts":
                timer_solution = sys.argv[i+1]
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
