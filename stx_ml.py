import network2
from stx_ml_loader import StxMlLoader
sml = StxMlLoader()
train, validation, test = sml.get_data()
net = network2.Network([48, 13], cost=network2.CrossEntropyCost)
net.SGD(train, 30, 10, 0.1, lmbda=5.0, evaluation_data=test, monitor_evaluation_accuracy=True)
