import math
import numpy as np
import operator
from stx_ml_loader import StxMlLoader


def euclideanDistance(instance1, instance2, length):
    return np.linalg.norm(np.array(instance1[4:]) - np.array(instance2[4:]))
    # distance = 0
    # for x in range(4, length):
    #     distance += pow((instance1[x] - instance2[x]), 2)
    # return math.sqrt(distance)


def getNeighbors(train, testInstance, k):
    distances = []
    length = len(testInstance)-1
    for x in range(len(train)):
	dist = euclideanDistance(testInstance, train[x], length)
	distances.append((train[x], dist))
    distances.sort(key=operator.itemgetter(1))
    neighbors = []
    for x in range(k):
	neighbors.append(distances[x][0])
    return neighbors


def getResponse(neighbors, ixx):
    classVotes = {}
    for x in range(len(neighbors)):
	response = neighbors[x][ixx]
	if response in classVotes:
	    classVotes[response] += 1
	else:
	    classVotes[response] = 1
    sortedVotes = sorted(classVotes.iteritems(), key=operator.itemgetter(1),
                         reverse=True)
    return sortedVotes[0][0]


def getAccuracy(test, predictions, ixx):
    correct = 0
    for x in range(len(test)):
	if test[x][ixx] == predictions[x]:
	    correct += 1
    return (correct / float(len(test))) * 100.0


def main():
    sml = StxMlLoader()
    sml = StxMlLoader(train_q="select * from ml where stk like 'R%' and dt between '2001-12-15' and '2002-02-08'", tst_q="select * from ml where stk like 'R%' and dt between '2002-02-08' and '2002-02-28'")
    train, validation, test = sml.get_raw_data()
    # generate predictions
    predictions=[]
    k = 15
    ixx = 3
    print('Predicted, Actual')
    n = 0
    m = len(test)
    res = {}
    for x in range(len(test)):
	neighbors = getNeighbors(train, test[x], k)
	result = getResponse(neighbors, ixx)
	predictions.append(result)
        n += 1
        if n % 100 == 0 or n == m:
            print('Processed {0:5d} out of {1:5d}'.format(n, m))
        actual = test[x][ixx]
        row = res.get(actual, [0, 0, 0])
        row[result + 1] += 1
        res[actual] = row
    for k, v in res.items():
        v_sum = float(np.sum(v))
        print('{0:d}:  {1:4.2f}  {2:4.2f}  {3:4.2f}'.
              format(k, v[0] / v_sum, v[1] / v_sum, v[2] / v_sum))
    accuracy = getAccuracy(test, predictions, ixx)
    print('Accuracy: ' + repr(accuracy) + '%')
	
main()
