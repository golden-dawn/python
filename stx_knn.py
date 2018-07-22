import numpy as np
import operator
from stx_ml_loader import StxMlLoader
import sys
import stxcal


def euclideanDistance(instance1, instance2, length):
    return np.linalg.norm(np.array(instance1[4:]) - np.array(instance2[4:]))
    # distance = 0
    # for x in range(4, length):
    #     distance += pow((instance1[x] - instance2[x]), 2)
    # return math.sqrt(distance)


def getNeighbors(train, testInstance, k, thresh):
    distances = []
    length = len(testInstance)-1
    stk = testInstance[0]
    dt = str(testInstance[1])
    for x in range(len(train)):
        if stk == train[x][0] and stxcal.num_busdays(
                str(train[x][1]), dt) < 180:
            continue
        dist = euclideanDistance(testInstance, train[x], length)
        distances.append((train[x], dist))
    distances.sort(key=operator.itemgetter(1))
    neighbors = []
    n_dict = {}
    m, d, x = 0, 0, 0
    while m < k and d < thresh:
        n_stk = distances[x][0][0]
        n_date = str(distances[x][0][1])
        d = distances[x][1]
        n_add = True
        if n_stk in n_dict:
            n_add = False
        else:
            if d < thresh:
                n_dict[n_stk] = n_date
            else:
                n_add = False
        if n_add:
            print('{0:s} {1:s} [{2:d}] Neighbor{3:d}: {4:s} - {5:s}, [{6:d}] '
                  'distance = {7:.2f}, add = {8:s}'.format(
                      stk, dt, testInstance[3], x, n_stk, n_date,
                      distances[x][0][3], d, str(n_add)))
        if n_add:
            neighbors.append(distances[x][0])
            m += 1
        x += 1
    return neighbors


def getResponse(neighbors, ixx, k):
    classVotes = {}
    for x in range(len(neighbors)):
        response = neighbors[x][ixx]
        if response in classVotes:
            classVotes[response] += 1
        else:
            classVotes[response] = 1
    sortedVotes = sorted(classVotes.iteritems(), key=operator.itemgetter(1),
                         reverse=True)
    print(sortedVotes)
    if len(sortedVotes) > 0:
        if sortedVotes[0][1] > k / 2:
            return sortedVotes[0][0]
    return 0


def getAccuracy(test, predictions, ixx):
    correct = 0
    for x in range(len(test)):
        if test[x][ixx] == predictions[x]:
            correct += 1
    return (correct / float(len(test))) * 100.0


def main():
    train_sd = sys.argv[1]
    train_ed = sys.argv[2]
    tst_sd = sys.argv[3]
    tst_ed = sys.argv[4]
    ixx = int(sys.argv[5])
    k = int(sys.argv[6])
    thresh = float(sys.argv[7])
    sml = StxMlLoader()

    train_q = "select stk, dt, pl_3, pl_5, jl11p1_time, jl11p1_dist, "\
        "jl11p2_time, jl11p2_dist, jl11p3_time, jl11p3_dist, jl11p4_time, "\
        "jl11p4_dist, jl11p4_udv, jl11p4_udd, jl16p1_time, jl16p1_dist, "\
        "jl16p2_time, jl16p2_dist, jl16p2_udv, jl16p2_udd, jl16p3_time, "\
        "jl16p3_dist, jl16p4_udv, jl16p4_udd, jl16p4_time, jl16p4_dist "\
        "from ml where stk like 'R%' and dt between '{0:s}' and '{1:s}'".\
        format(train_sd, train_ed)
    tst_q = "select stk, dt, pl_3, pl_5, jl11p1_time, jl11p1_dist, "\
        "jl11p2_time, jl11p2_dist, jl11p3_time, jl11p3_dist, jl11p4_time, "\
        "jl11p4_dist, jl11p4_udv, jl11p4_udd, jl16p1_time, jl16p1_dist, "\
        "jl16p2_time, jl16p2_dist, jl16p2_udv, jl16p2_udd, jl16p3_time, "\
        "jl16p3_dist, jl16p4_udv, jl16p4_udd, jl16p4_time, jl16p4_dist "\
        "from ml where stk='R' and dt between '{0:s}' and '{1:s}'".\
        format(tst_sd, tst_ed)

    # train_q = "select * from ml where stk like 'R%' and dt between "\
    #     "'{0:s}' and '{1:s}'".format(train_sd, train_ed)
    # tst_q = "select * from ml where stk like 'R%' and dt between "\
    #     "'{0:s}' and '{1:s}'".format(tst_sd, tst_ed)
    sml = StxMlLoader(train_q=train_q, tst_q=tst_q)

    train, validation, test = sml.get_raw_data()
    # generate predictions
    predictions = []
    print('ixx = {0:d}, k = {1:d}'.format(ixx, k))
    n = 0
    m = len(test)
    res = {}
    for x in range(len(test)):
        neighbors = getNeighbors(train, test[x], k, thresh)
        result = getResponse(neighbors, ixx, k)
        if result is not None:
            predictions.append(result)
            actual = test[x][ixx]
            row = res.get(actual, [0, 0, 0])
            row[result + 1] += 1
            res[actual] = row
        n += 1
        if n % 1000 == 0 or n == m:
            print('Processed {0:5d} out of {1:5d}'.format(n, m))

    for k, v in res.items():
        v_sum = float(np.sum(v))
        print('{0:d}:  {1:4.2f}  {2:4.2f}  {3:4.2f}  {4:4.0f}'.
              format(k, v[0] / v_sum, v[1] / v_sum, v[2] / v_sum, v_sum))
    accuracy = getAccuracy(test, predictions, ixx)
    print('Accuracy: ' + repr(accuracy) + '%')


main()