# Sample simulator demo
# Miguel Matos - miguel.marques.matos@tecnico.ulisboa.pt
# (c) 2012-2018

LOG_TO_FILE=False

from collections import defaultdict
import math
import random
import sys
import os
import yaml
import _pickle as cPickle
import logging

from sim import sim
import utils

def init():
    global nodeState

    # schedule execution for all nodes
    for nodeId in nodeState:
        sim.schedulleExecution(CYCLE,nodeId)

    # other things such as periodic measurements can also be scheduled
    # to schedulle periodic measurements use the following
    # for c in range(nbCycles * 10):
    #    sim.schedulleExecutionFixed(MEASURE_X, nodeCycle * (c + 1))


def CYCLE(myself):
    global nodeState
    
    # with churn the node might be gone
    if myself not in nodeState:
        return

    # show progress for one node
    if myself == 0:
        logger.info('node {} cycle {}'.format(myself,nodeState[myself][CURRENT_CYCLE]))

    nodeState[myself][CURRENT_CYCLE] += 1

    # schedule next execution
    if nodeState[myself][CURRENT_CYCLE] < nbCycles:
        sim.schedulleExecution(CYCLE,myself)

    # select random node to send message
    # assume global view
    if random.random() <= probBroadcast:
        target = random.choice(nodeState.keys())
        sim.send(HELLO, target, myself, "hello, i am {}".format(myself),"hello")

        nodeState[myself][MSGS_SENT]+=1

def HELLO(myself, source, msg1,msg2):
    global nodeState

    logger.info("Node {} Received {} from {} with {}".format(myself,msg1,source,msg2))
    nodeState[myself][MSGS_RECEIVED] += 1
    sim.send(HELLO_REPLY, source, myself, "hello reply, i am {}".format(myself),"hello to you too")


def HELLO_REPLY(myself, source, msg1,msg2):
    global nodeState

    logger.info("Node {} Received {} from {} with {}".format(myself,msg1,source,msg2))
    nodeState[myself][MSGS_RECEIVED] += 1

def wrapup():
    global nodeState
    logger.info("Wrapping up")
    logger.info(nodeState)

    receivedMessages = map(lambda x : nodeState[x][MSGS_RECEIVED], nodeState)
    sentMessages = map(lambda x : nodeState[x][MSGS_SENT], nodeState)

    # gather some stats, see utils for more functions
    logger.info("receivedMessages {}".format(receivedMessages))
    logger.info("receivedMessages min: {}, max: {}, total: {}".format(min(receivedMessages),max(receivedMessages),sum(receivedMessages)))
    logger.info("sentMessages {}".format(sentMessages))
    logger.info("sentMessages min: {}, max: {}, total: {}".format(min(sentMessages),max(sentMessages),sum(receivedMessages)))

    # dump data into gnuplot format
    utils.dumpAsGnuplot([receivedMessages, sentMessages], dumpPath + '/messages-' + str(runId) + '.gpData', ['#receivedMessages sentMessages'])

    # dump data for later processing
    with open(dumpPath + '/dumps-' + str(runId) + '.obj', 'w') as f:
        cPickle.dump(receivedMessages, f)
        cPickle.dump(sentMessages, f)

def createNode():
    # maintain the node state as a list with the required variables
    # a dictionary is more readable but performance drop is considerable
    global CURRENT_CYCLE
    global MSGS_RECEIVED
    global MSGS_SENT

    CURRENT_CYCLE, MSGS_RECEIVED, MSGS_SENT = 0,1,2

    return [ 0, 0 , 0 ]


def configure(config):
    global nbNodes, nbCycles, probBroadcast, nodeState, nodeCycle 

    IS_CHURN = config.get('CHURN', False)
    if IS_CHURN:
        CHURN_RATE = config.get('CHURN_RATE', 0.)
    MESSAGE_LOSS = float(config.get('MESSASE_LOSS', 0))
    if MESSAGE_LOSS > 0:
        sim.setMessageLoss(MESSAGE_LOSS)

    nbNodes = config['nbNodes']
    probBroadcast = config['probBroadcast']
    nbCycles = config['nbCycles']

    IS_CHURN = config.get('CHURN', False)

    latencyTablePath = config['LATENCY_TABLE']
    latencyValue = None
    try:
        with open(latencyTablePath, 'r') as f:
            latencyTable = cPickle.load(f)
    except:
        latencyTable = None
        latencyValue = int(latencyTablePath)
        logger.warn('Using constant latency value: {}'.format(latencyValue) ) 

    latencyTable = utils.checkLatencyNodes(latencyTable, nbNodes, latencyValue)
    latencyDrift = eval(config['LATENCY_DRIFT'])

    IS_CHURN = config.get('CHURN', False)

    nodeCycle = int(config['NODE_CYCLE'])
    rawNodeDrift = float(config['NODE_DRIFT'])
    nodeDrift = int(nodeCycle * float(config['NODE_DRIFT']))

    nodeState = defaultdict()
    for n in range(nbNodes):
        nodeState[n] = createNode()

    sim.init(nodeCycle, nodeDrift, latencyTable, latencyDrift)



if __name__ == '__main__':

    #setup logger
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
            #logging.basicConfig(format='%(asctime)s %(message)s', level=logging.DEBUG, filename='logs/echo.log', filemode='w')
    dumpPath = sys.argv[1]
    confFile = dumpPath + '/conf.yaml'
    runId = int(sys.argv[2])
    f = open(confFile)

    #load configuration file
    configure(yaml.load(f))
    logger.info('Configuration done')

    #start simulation
    init()
    logger.info('Init done')
    #run the simulation
    sim.run()
    logger.info('Run done')
    #finish simulation, compute stats
    wrapup()
    logger.info("That's all folks!")
