import sys
import json
import pprint
import random
import time
import threading
from commlib.node import Node as CommlibNode
from commlib.transports.mqtt import ConnectionParameters

class Node:
    def __init__(self, data):
        self.data = data
        self.id = data['id']
        self.label = data['data']['label']
        self.toolbox = data['data']['toolbox']
        self.count = data['data']['count']
        self.parameters = data['data']['parameters'] if 'parameters' in data['data'] else []
        self.connections = {}
        self.is_preempted = False
        # In case of thread split, we need to keep the executors
        self.executors = {}
        # In case of thread join, we need to keep the next join node
        self.nextJoin = None
        # In case of preempt, we need to keep the executor to kill
        self.executor_to_preempt = None

    def addConnection(self, node, connection):
        self.connections[node.id] = connection

    def execute(self):
        if self.is_preempted:
            print("Node: ", self.id, " ", self.label, " is preempted")
            return None

        if self.label == "Condition":
            # Select one of the outputs at random
            print("Executing node: ", self.id, " ", self.label)
            time.sleep(1)
            l = random.randint(0, len(self.connections) - 1)
            return list(self.connections.keys())[l]
        elif self.label == "End":
            return None
        elif self.label == "Thread split":
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
        elif self.label == "Preempt":
            # Enforce preemption
            print("Executing node: ", self.id, " ", self.label)
            time.sleep(1)
            self.executor_to_preempt.enforcePreemption()
            return list(self.connections.keys())[0]
        else:
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

class NodeExecutor:
    def __init__(self, execType):
        self.starting_node = None
        self.is_preempted = False
        self.execType = execType
        self.nodes = {}
        self.runner = None
        self.finished = False
        self.to_preempt = False
        self.is_preempted = False

    def addNode(self, node):
        self.nodes[node.id] = node

    def setStartingNode(self, node_id):
        self.starting_node = node_id

    def execute(self):
        print("Executor: ", self.execType, " started")
        # Make the is_preempted flag of all nodes false
        for n in self.nodes:
            self.nodes[n].is_preempted = False
        self.finished = False
        self.runner = self.starting_node
        while self.runner != None and self.runner in self.nodes:
            self.runner = self.nodes[self.runner].execute()
            print("Runner: ", self.runner)
        print("Executor: ", self.execType, " finished")
        self.finished = True

    def executeThreaded(self):
        threading.Thread(target=self.execute).start()

    def enforcePreemption(self):
        self.is_preempted = True
        for n in self.nodes:
            self.nodes[n].is_preempted = True
        # No need to finish this, the next node will take care of it

class AppMakerExecutor:
    def __init__(self):
        self.nodes = {}
        self.store = {}
        self.node_executors = {}
        self.nodes_assigned_to_executors = {}

        conn_params = ConnectionParameters(
            host='broker.emqx.io',
            port=1883,
        )

        self.commlib_node = CommlibNode(node_name='locsys.app_executor_node',
                connection_params=conn_params,
                heartbeats=False,
                debug=True)
        
        self.commlib_node.create_subscriber(
            topic="locsys/app_executor/deploy", 
            on_message=self.on_message
        )

    def on_message(self, message):
        print("Received model")
        self.load_model(message)
        self.execute()

    def findCoorespondingThreadJoin(self, thread_split_id):
        # Find the corresponding thread join node
        neighbors = [self.nodes[thread_split_id].connections[x]['target'] for x in self.nodes[thread_split_id].connections]
        # Create a set of neighbors to avoid loops
        # NOTE NOTE NOTE
        while True:
            temp_neighbors = []
            print(">>", neighbors)
            for n in neighbors:
                if self.nodes[n].label == "Thread join":
                    return n
                else:
                    temp_neighbors += [self.nodes[n].connections[x]['target'] for x in self.nodes[n].connections]
            neighbors = temp_neighbors
            
        return None

    def executorUpdate(self, node_id, executor_id):
        # Check if the node is a terminator (unless the executor starts from the terminator)
        if self.nodes[node_id].label == "End": 
            print("The end was found: ", node_id, " ", self.nodes[node_id].label)
            return
        elif self.nodes[node_id].label == "Thread split" and node_id != executor_id:
            print("The thread split was found: ", node_id, " ", self.nodes[node_id].label)
            # Find the corresponding thread join node
            thread_join_id = self.findCoorespondingThreadJoin(node_id)
            self.nodes[node_id].nextJoin = thread_join_id
            # Add the node to the executor
            self.node_executors[executor_id].addNode(self.nodes[thread_join_id])
            print("Thread join added to executor: ", executor_id)
            self.nodes_assigned_to_executors[thread_join_id] = executor_id
            # Update the executor
            self.executorUpdate(thread_join_id, executor_id)
        else:
            print("Not stopping! Going for neighbors of ", node_id)
            # Find neighbors ids of the node
            neighbors = self.nodes[node_id].connections
            # Assign the neighbors to the executor
            for n in neighbors:
                print("Neighbor: ", n, " ", self.nodes[n].label)
                if n not in self.nodes_assigned_to_executors:
                    print("Neighbor not assigned to executor yet, going for it")
                    self.nodes_assigned_to_executors[n] = executor_id
                    self.node_executors[executor_id].addNode(self.nodes[n])
                    print("Node", n, "added to executor: ", executor_id)
                    self.executorUpdate(n, executor_id)
            
    def load_model(self, model):
        # Load the model from the file
        nodes = model['nodes']
        edges = model['edges']
        # store = model['store']

        for n in nodes:
            n = Node(n)
            self.nodes[n.id] = n

        for e in edges:
            self.nodes[e['source']].addConnection(self.nodes[e['target']], e)

        for n in self.nodes:
            self.nodes[n].printNode()
            print("\n")

        # Find Start nodes
        print("Starting executor finder")
        for id in self.nodes:
            if self.nodes[id].label == "Start":
                self.node_executors[id] = NodeExecutor("start")
                self.node_executors[id].setStartingNode(id)
                self.node_executors[id].addNode(self.nodes[id])
                self.nodes_assigned_to_executors[id] = id
                print("Starting executor found: ", id, " ", self.nodes[id].label)
                self.executorUpdate(id, id)

            if self.nodes[id].label == "Thread split":
                # Start an executor from its neighbors
                # Find neighbors ids of the node
                neighbors = self.nodes[id].connections
                print("Thread split found: ", id, " ", self.nodes[id].label) 
                for n in neighbors:
                    if n not in self.nodes_assigned_to_executors:
                        self.node_executors[n] = NodeExecutor("thread")
                        self.node_executors[n].setStartingNode(n)
                        self.node_executors[n].addNode(self.nodes[n])
                        self.nodes_assigned_to_executors[n] = n
                        print("Thread executor started: ", n, " ", self.nodes[n].label)
                        self.executorUpdate(n, n)
                        # Addinng the threads as executors to node
                        self.nodes[id].executors[n] = self.node_executors[n]

        print("Executors: ")
        for e in self.node_executors:
            print("Executor: ", e, " ", self.node_executors[e].execType)
            print("Nodes: ")
            for n in self.node_executors[e].nodes:
                print("\t", n, " ", self.node_executors[e].nodes[n].label)
            print("\n")

        # Find the preempt nodes
        for id in self.nodes:
            if self.nodes[id].label == "Preempt":
                thread_parameter = self.nodes[id].parameters[0]['value']
                # Split the thread parameter by :
                thread_parameter = thread_parameter.split(":")
                # Trim the spaces and make them integers
                thread_parameter = [int(x.strip()) for x in thread_parameter]
                node_count = thread_parameter[0]
                thread_count = thread_parameter[1]
                # Find the node with count equal to node_count
                for n in self.nodes:
                    if self.nodes[n].count == node_count:
                        # Find the next node of the thread with count equal to thread_count
                        for c in self.nodes[n].connections:
                            print(c, self.nodes[n].connections[c])
                            if self.nodes[n].connections[c]['sourceHandle'] == f"out_{thread_count}":
                                self.nodes[id].executor_to_preempt = self.node_executors[c]
                                print("Executor to preempt: ", c, " ", self.nodes[c].label)
                                break

    def execute(self):
        # Find the start executor and deploy it
        for e in self.node_executors:
            if self.node_executors[e].execType == "start":
                self.node_executors[e].execute()
                break
        print("Execution finished")

if __name__ == "__main__":
    amexe = AppMakerExecutor()
    print("AppMakerExecutor loaded")

    amexe.commlib_node.run_forever()