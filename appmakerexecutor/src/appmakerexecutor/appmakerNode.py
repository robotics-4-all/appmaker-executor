import time
import random

class Node:
    def __init__(self, data, publisher=None):
        self.data = data
        self.publisher = publisher
        self.id = data['id']
        self.label = data['data']['label']
        self.toolbox = data['data']['toolbox']
        self.count = data['data']['count']
        self.parameters = data['data']['parameters'] if 'parameters' in data['data'] else []
        self.connections = {}
        self.connection_list = []
        self.is_preempted = False
        # In case of thread split, we need to keep the executors
        self.executors = {}
        # In case of thread join, we need to keep the next join node
        self.nextJoin = None
        # In case of preempt, we need to keep the executor to kill
        self.executor_to_preempt = None

    def addConnection(self, node, connection):
        self.connections[node.id] = connection
        self.connection_list.append(connection['target'])

    def publish(self, message):
        if self.publisher != None:
            self.publisher.publish({
                "node_id": self.id,
                "message": message
            })

    def execute(self):
        next_node = None
        if self.is_preempted:
            print("Node: ", self.id, " ", self.label, " is preempted")
            return None

        self.publish("start")
        if self.label == "Condition":
            next_node = self.executeCondition()
        elif self.label == "Random":
            next_node = self.executeRandom()
        elif self.label == "End":
            next_node = None
        elif self.label == "Thread split":
            next_node = self.executeThreadSplit()
        elif self.label == "Preempt":
            next_node = self.executePreempt()
        elif self.label == "Delay":
            next_node = self.executeDelay()
        else: # All other nodes
            next_node = self.executeGeneral()
        
        self.publish("end")
        return next_node
    
    def executeCondition(self):
        # Select one of the outputs at random
        print("Executing node: ", self.id, " ", self.label)
        next_node_index = 0
        for p in self.parameters:
            print(p['id'], " ", p['value'])
            # Evaluate the condition
            result = False
            try:
                result = eval(str(p['value']))
                print("Result: ", result)
            except Exception as e:
                print("Error in evaluating the condition", e) 
            if result:
                break
            next_node_index += 1 
        time.sleep(1)
        return list(self.connections.keys())[next_node_index]

    def executeRandom(self):
        # Select one of the outputs at random
        print("Executing node: ", self.id, " ", self.label)
        time.sleep(1)
        # Gather all the parameters
        probabilities = [float(x['value']) for x in self.parameters]
        prob_sum = sum(probabilities)
        random_prob = random.uniform(0, prob_sum)
        print(self.connection_list)
        print("Random probability: ", random_prob)
        for i in range(len(probabilities)):
            if random_prob < probabilities[i]:
                print("Selected: ", i)
                return self.connection_list[i]
            random_prob -= probabilities[i]
        print("Something went wrong, returning the last connection")
        return self.connection_list[-1]
    
    def executeThreadSplit(self):
        # We must start the executors threaded
        time.sleep(1)
        print("Executing node: ", self.id, " ", self.label)
        if self.executors:
            print("Executing threads")
            for e in self.executors:
                self.executors[e].finished = False
                self.executors[e].executeThreaded()
            print("Waiting for threads to finish") 
            while True:
                time.sleep(0.1)
                if all([self.executors[e].finished for e in self.executors]):
                    print("Threads finished")
                    break
        return self.nextJoin
    
    def executePreempt(self):
        # Enforce preemption
        print("Executing node: ", self.id, " ", self.label)
        time.sleep(1)
        self.executor_to_preempt.enforcePreemption()
        return list(self.connections.keys())[0]
    
    def executeDelay(self):
        # Wait for the delay time
        print("Executing node: ", self.id, " ", self.label)
        print(self.parameters)
        if 'value' not in self.parameters[0]:
            print("Delay parameter not found")
            return None
        print("Delay: ", self.parameters[0]['value'])
        time.sleep(float(self.parameters[0]['value']))
        return list(self.connections.keys())[0]
    
    def executeGeneral(self):
        print("Executing node: ", self.id, " ", self.label)
        time.sleep(1)
        return list(self.connections.keys())[0]

    def printNode(self):
        print("Node: ", self.id, " ", self.label, " ", self.count)
        print("Parameters: ")
        for p in self.parameters:
            print("\t", p['id'], " ", p['value'] if 'value' in p else "")
        print("Connections: ")
        for c in self.connections:
            print("\tto ", c)
