import network2
from stx_ml_loader import StxMlLoader
sml = StxMlLoader()
sml = StxMlLoader(train_q="select * from ml where stk like 'R%' and dt between '2001-01-01' and '2002-02-08'", tst_q="select * from ml where stk like 'R%' and dt between '2002-02-08' and '2002-12-31'")
train, validation, test = sml.get_data()
# net = network2.Network([48, 50, 13], cost=network2.CrossEntropyCost)
net = network2.Network([48, 50, 13], cost=network2.StxCost)
net.SGD(train, 30, 10, 0.1, lmbda=5.0, evaluation_data=test, monitor_training_cost=True, monitor_training_accuracy=True, monitor_evaluation_accuracy=True)
# net.SGD(train, 30, 10, 0.1, lmbda=5.0, evaluation_data=test, monitor_training_cost=True, monitor_training_accuracy=True, monitor_evaluation_cost=True, monitor_evaluation_accuracy=True)


import network2
from stx_ml_loader import StxMlLoader
sml = StxMlLoader()
sml = StxMlLoader(train_q="select * from ml where stk like 'R%' and dt between '2001-11-01' and '2002-02-08'", tst_q="select * from ml where stk like 'R%' and dt between '2002-02-08' and '2002-03-31'")
train, validation, test = sml.get_data()
net = network2.Network([48, 50, 3], cost=network2.CrossEntropyCost)
net.SGD(train, 30, 10, 0.01, lmbda=1.0, evaluation_data=test, monitor_training_cost=True, monitor_training_accuracy=True, monitor_evaluation_accuracy=True)

import network2
from stx_ml_loader import StxMlLoader
sml = StxMlLoader()
sml = StxMlLoader(train_q="select stk, dt, pl_3, pl_5, jl11p1_time, jl11p1_dist, jl11p2_time, jl11p2_dist, jl11p3_time, jl11p3_dist, jl11p4_time, jl11p4_dist, jl11p4_udv, jl11p4_udd, jl16p1_time, jl16p1_dist, jl16p2_time, jl16p2_dist, jl16p2_udv, jl16p2_udd, jl16p3_time, jl16p3_dist, jl16p4_udv, jl16p4_udd, jl16p4_time, jl16p4_dist from ml where stk like 'R%' and dt between '2001-11-01' and '2002-02-08'", tst_q="select stk, dt, pl_3, pl_5, jl11p1_time, jl11p1_dist, jl11p2_time, jl11p2_dist, jl11p3_time, jl11p3_dist, jl11p4_time, jl11p4_dist, jl11p4_udv, jl11p4_udd, jl16p1_time, jl16p1_dist, jl16p2_time, jl16p2_dist, jl16p2_udv, jl16p2_udd, jl16p3_time, jl16p3_dist, jl16p4_udv, jl16p4_udd, jl16p4_time, jl16p4_dist from ml where stk like 'R%' and dt between '2002-02-08' and '2002-03-31'")
ll = len(train[0][0])
train, validation, test = sml.get_data()
net = network2.Network([ll, 50, 3], cost=network2.CrossEntropyCost)
net.SGD(train, 30, 10, 0.01, lmbda=1.0, evaluation_data=test, monitor_training_cost=True, monitor_training_accuracy=True, monitor_evaluation_accuracy=True)

#stk, dt, pl_3, pl_5, jl11p1_time, jl11p1_dist, jl11p2_time, jl11p2_dist, jl11p3_time, jl11p3_dist, jl11p4_time, jl11p4_dist, jl11p4_udv, jl11p4_udd, jl16p1_time, jl16p1_dist, jl16p2_time, jl16p2_dist, jl16p2_udv, jl16p2_udd, jl16p3_time, jl16p3_dist, jl16p4_udv, jl16p4_udd, jl16p4_time, jl16p4_dist


import network2
from stx_ml_loader import StxMlLoader
sml = StxMlLoader()
sml = StxMlLoader(train_q="select * from ml where stk = 'RIG' and dt between '2002-01-01' and '2002-02-08'", tst_q="select * from ml where stk = 'RIG' and dt between '2002-02-08' and '2002-03-01'")
n = 4
train, validation, test = sml.get_data(n)
net = network2.Network([n, 50, 3], cost=network2.CrossEntropyCost)
net.SGD(train, 30, 1, 0.01, lmbda=1.0, evaluation_data=test, monitor_training_cost=True, monitor_training_accuracy=True, monitor_evaluation_accuracy=True)


# select stk, dt, pl_3, pl_5, jl06p1_time, jl06p1_dist, jl06p1_udv, jl06p1_udd from ml where stk='RIG' and dt between '2002-01-01' and '2002-02-08';

'''
nabla_b = [np.zeros(b.shape) for b in net2.biases]
nabla_w = [np.zeros(w.shape) for w in net2.weights]

x, y = mini_batch[0]
delta_nabla_b, delta_nabla_w = net2.backprop(x, y)
nabla_b = [nb+dnb for nb, dnb in zip(nabla_b, delta_nabla_b)]
nabla_w = [nw+dnw for nw, dnw in zip(nabla_w, delta_nabla_w)]

x, y = mini_batch[1]
print 'x', x
print 'y', y

b_nabla = [np.zeros(b.shape) for b in net2.biases]
w_nabla = [np.zeros(w.shape) for w in net2.weights]
activation = x
activations = [x]
zs = []

for b, w in zip(net2.biases, net2.weights):
            print 'activation = ', activation
            print 'w = ', w
            z1 = np.dot(w, activation)
            print 'z1 = ', z1
            print 'b', b
            z = z1 + b
            print 'z', z
            zs.append(z)
            activation = sigmoid(z)
            print 'activation', activation
            activations.append(activation)
# backward pass
delta = net2.cost_derivative(activations[-1], y) * sigmoid_prime(zs[-1])
b_nabla[-1] = delta
w_nabla[-1] = np.dot(delta, activations[-2].transpose())




for x, y in mini_batch:
            delta_nabla_b, delta_nabla_w = net2.backprop(x, y)
            nabla_b = [nb+dnb for nb, dnb in zip(nabla_b, delta_nabla_b)]
            nabla_w = [nw+dnw for nw, dnw in zip(nabla_w, delta_nabla_w)]
net2.weights = [w-(eta/len(mini_batch))*nw for w, nw in zip(net2.weights, nabla_w)]
net2.biases = [b-(eta/len(mini_batch))*nb for b, nb in zip(net2.biases, nabla_b)]

'''
