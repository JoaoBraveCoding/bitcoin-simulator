# Misc. stats and plotting utils.
# Miguel Matos - mm@gsd.inesc-id.pt
# (c) 2012-2017

import math
from cookielib import logger
import numpy
import cPickle


def computeCDF(data, precision=1000):
    from scipy.stats import cumfreq, scoreatpercentile
    maxVal = max(data) + 0.

    freqs, _, _, _ = cumfreq(data, precision)

    freqsNormalized = map(lambda x: x / maxVal, freqs)
    values = []

    step = 100. / precision

    scores = numpy.arange(0, 100 + step, step)
    for s in scores:
        values.append(scoreatpercentile(data, s))

    return values, freqs, freqsNormalized


def dump_as_gnu_plot(data, path, caption, pad=True):
    if pad:
        merge = zip(range(len(data[0])), *data)
    else:
        merge = zip(*data)

    writer = open(path, 'w')
    print>> writer, caption

    for pack in merge:
        strData = str(pack)  # to avoid commas and brackets
        strData = strData[1:-1].replace(',', '\t').replace(' ', '')
        print>> writer, strData

    writer.close()


def pad_lists_to_same_size(data, defaultElement=0):
    maxSize = 0

    for lst in data:
        lgt = len(lst)
        if lgt > maxSize:
            maxSize = lgt

    return map(lambda x: [defaultElement for n in range(maxSize - len(x))] + x, data)


def compute_average_of_message(variable_name, variable, dumpPath):
    # dump data for later processing
    with open(dumpPath + '/'+ variable_name +'-avg.txt', 'a') as f:
        list = map(lambda x: variable[x][0], range(0, len(variable)))
        avg_sent = compute_average(list)
        list = map(lambda x: variable[x][1], range(0, len(variable)))
        avg_received = compute_average(list)
        print >> f, len(variable), avg_sent, avg_received


def compute_average(data):
    """
    Computes the average of a list.
    """
    items = len(data)

    if items > 1:
        print('Averaging...')
        dataAverage = data[0] + data[1]

        for i in range(2, items):
            dataAverage += data[i]

        dataAverage = dataAverage/items
    elif items == 1:
        print('Single Run.')
        dataAverage = data[0]
    else:
        dataAverage = 0

    return dataAverage


def mean(data):
    try:
        if len(data) > 0:
            return sum(data) / (len(data) + 0.)
        else:
            return 0
    except Exception:  # we may receive a int or float, return it directly if that's the case
        return data


def get_closest(data, value):
    """
    Finds the index of the closest element to value in data. Data should be ordered.
    """
    data.sort()
    i = 0
    lgt = len(data)
    while i < lgt and value > data[i]:
        i += 1
    return i if i < lgt else lgt - 1


# original from scipy, adjusted to run on pypy
def score_at_percentile(a, per, limit=(), isSorted=False):
    # values = np.sort(a,axis=0) #not yet implemented in pypy
    if not isSorted:
        values = sorted(a)  # np.sort(a,axis=0)
    else:
        values = a

    if limit:
        values = values[(limit[0] <= values) & (values <= limit[1])]

    # idx = per /100. * (values.shape[0] - 1)
    idx = per / 100. * (len(values) - 1)
    if idx % 1 == 0:
        return values[int(idx)]
    else:
        return _interpolate(values[int(idx)], values[int(idx) + 1], idx % 1)


def _interpolate(a, b, fraction):
    """Returns the point at the given fraction between a and b, where
    'fraction' must be between 0 and 1.
    """
    return a + (b - a) * fraction


def percentiles(data, percs=[0, 1, 5, 25, 50, 75, 95, 99, 100], paired=True, roundPlaces=None):
    """
    Returns the values at the given percentiles.
    Inf percs is null gives the 5,25,50,75,95,99,100 percentiles.
    """

    data = sorted(data)
    # data might be an iterator so we need to do this check after sorting
    if len(data) == 0:
        return []
    result = []

    for p in percs:
        score = score_at_percentile(data, p, isSorted=True)
        if roundPlaces:
            score = round(score, roundPlaces)
        if paired:
            result.append((p, score))
        else:
            result.append(score)

    return result


def check_latency_nodes(latencyTable, nbNodes, defaultLatency=None):
    global latencyValue

    if latencyTable is None and defaultLatency is not None:
        print 'WARNING: using constant latency'
        latencyTable = {n: {m: defaultLatency for m in range(nbNodes)} for n in range(nbNodes)}
        # latencyTable = {n : {m: random.randint(0,defaultLatency)for m in range(nbNodes)} for n in range(nbNodes) }
        return latencyTable

    nbNodesAvailable = len(latencyTable)

    latencyList = [l for tmp in latencyTable.itervalues() for l in tmp.values()]
    latencyValue = math.ceil(percentiles(latencyList, percs=[50], paired=False)[0])

    if nbNodes > nbNodesAvailable:
        nodesToPopulate = nbNodes - nbNodesAvailable

        nodeIds = range(nbNodes)

        logger.warning('Need to add nodes to latencyTable')
        for node in range(nbNodesAvailable):
            latencyTable[node].update({target: numpy.random.choice(latencyList) for target in nodeIds[nbNodesAvailable:]})

        for node in range(nbNodesAvailable, nbNodes):
            latencyTable[node] = {target: numpy.random.choice(latencyList) for target in nodeIds}
            latencyTable[node].pop(node)  # remove itself
        # FIXME: we should also remove some other nodes to be more faithful to the original distribution

        with open('/tmp/latencyTable.obj', 'w') as f:
            cPickle.dump(latencyTable, f)

    return latencyTable


def copy(org):
    """
    Much, much faster than deepcopy, for a dict of the simple python types.
    """
    out = dict().fromkeys(org)

    for k, v in org.iteritems():
        try:
            out[k] = v.copy()  # dicts, sets
        except AttributeError:
            try:
                out[k] = v[:]  # lists, tuples, strings, unicode
            except TypeError:
                out[k] = v  # ints
    return out
