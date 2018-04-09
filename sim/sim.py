# Sim core. It maintains a set of events in a heap, delivering them in order
# to the application.
# Miguel Matos - mm@gsd.inesc-id.pt
# (c) 2012-2017

import sys
import heapq
import random

messagesTotal = 0
messagesSent = 0
messagesDropped = 0

queue = []
cnt = 0
timestamp = 0

NODE_CYCLE = None
NODE_DRIFT = None

LATENCY_TABLE = None
LATENCY_DRIFT = None


def getNodeNextExecution():
    return random.randint(NODE_CYCLE - NODE_DRIFT, NODE_CYCLE + NODE_DRIFT)


def getMessageLatency(orig, dest, wrap=True):
    if wrap:
        lat = LATENCY_TABLE.get(orig % LATENCY_TABLE_LENGHT).get(dest % LATENCY_TABLE_LENGHT, 0)
    else:
        lat = LATENCY_TABLE.get(orig).get(dest, 0)
    # print 'LATENCY_DRIFT: ', LATENCY_DRIFT
    if LATENCY_DRIFT and LATENCY_DRIFT != 0:
        raise NotImplementedError("TODO")
    # lat = int(random.randim( lat - (lat * LATENCY_DRIFT  ), lat + (lat * LATENCY_DRIFT    ) ) )

    return lat


def getNumberEvents():
    return len(queue)


def setMessageLoss(rate):
    global messageLoss, send, messagesDropped, messagesSent, messagesTotal

    print('setting message loss to: ', rate)
    messagesDropped = messagesSent = messagesTotal = 0
    messageLoss = rate
    send = sendLossy


def sendLossy(f, *p):
    global queue, timestamp, messagesDropped, messagesSent, messagesTotal

    messagesTotal += 1

    if random.random() > messageLoss:
        lat = getMessageLatency(p[1], p[0])
        # target is reachable or is itself
        if lat > 0 or p[1] == p[0]:
            ts = timestamp + lat
            heapq.heappush(queue, (ts, (f, p)))
            messagesSent += 1
    # elif p[1] == p[0]: #message to self
    #  ts = timestamp + lat
    #  heapq.heappush(queue,(ts,(f,p)) )
    #  print 'zero lat: ', p[1], p[0]
    else:
        messagesDropped += 1


def sendReliable(f, *p):
    global queue, timestamp

    try:
        lat = getMessageLatency(p[1], p[0])
        if lat > 0 or p[1] == p[0]:
            ts = timestamp + lat
            heapq.heappush(queue, (ts, (f, p)))
    except Exception as e:
        print(p[1])
        print(p[0])
        print(e)
        sys.exit()


def schedulleExecutionFixed(f, delta):
    global queue, timestamp
    heapq.heappush(queue, (timestamp + delta, (f, [])))


def schedulleExecution(f, node):
    global queue, timestamp

    ts = timestamp + getNodeNextExecution()
    # prevent executing in the past
    if ts <= timestamp:
        ts = timestamp + 1

    heapq.heappush(queue, (ts, (f, [node])))


def schedulleExecutionBounded(f, node, cycle):
    # return random.randint(NODE_CYCLE-NODE_DRIFT, NODE_CYCLE+NODE_DRIFT)
    ts = random.randint(NODE_CYCLE * cycle - NODE_DRIFT, NODE_CYCLE * cycle + NODE_DRIFT)
    # prevent executing in the past
    if ts <= timestamp:
        ts = timestamp + 1
    heapq.heappush(queue, (ts, (f, [node])))


def run():
    global queue, timestamp, cnt, messagesSent, messagesDropped, messagesTotal

    try:
        while True:
            try:
                # values = heapq.heappop(queue)
                # timestamp,(f,p) = values
                timestamp, (f, p) = heapq.heappop(queue)
            except IndexError as e:
                print('empty queue')
                break

            f(*p)
        # cnt+=1
        # if cnt%1000==0:
        # 	logger.info(" {} events queued \t {} events done\t timestamp: %d           ".format(len(queue),cnt,timestamp))
    except Exception as e:
        print("error", e)
        print("queue", queue)
        print('Executed: %d events' % (cnt))
        sys.exit(10)


send = sendReliable


def init(nodeCycle, nodeDrift, latencyTable, latencyDrift=0):
    global NODE_CYCLE, NODE_DRIFT, LATENCY_TABLE, LATENCY_DRIFT, LATENCY_TABLE_LENGHT

    NODE_CYCLE = nodeCycle
    NODE_DRIFT = nodeDrift

    LATENCY_TABLE = latencyTable
    LATENCY_DRIFT = latencyDrift
    LATENCY_TABLE_LENGHT = len(latencyTable)
