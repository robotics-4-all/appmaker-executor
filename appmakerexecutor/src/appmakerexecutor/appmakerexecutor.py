import sys
import json
import pprint

class Node:
    def __init__(self, data):
        self.data = data
        self.id = data['id']
        self.label = data['data']['label']
        self.toolbox = data['data']['toolbox']
        self.count = data['data']['count']
        self.parameters = data['data']['parameters'] if 'parameters' in data['data'] else []
        self.connections = {}

    def addConnection(self, node, connection):
        self.connections[node.id] = connection

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

    def addNode(self, node):
        self.nodes[node.id] = node

    def setStartingNode(self, node_id):
        self.starting_node = node_id

    def execute(self):
        pass

class AppMakerExecutor:
    def __init__(self, model_file):
        self.appmaker_model = model_file
        self.nodes = {}
        self.store = {}
        self.node_executors = {}
        self.nodes_assigned_to_executors = {}
        
        self.load_model()
        print("AppMakerExecutor initialized with model: ", self.appmaker_model)

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
            
    def load_model(self):
        # Load the model from the file
        with open(self.appmaker_model, 'r') as f:
            model = json.load(f)
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

            # if self.nodes[id].label == "Thread join":
            #     self.node_executors[id] = NodeExecutor("thread_join")
            #     self.node_executors[id].setStartingNode(id)
            #     self.node_executors[id].addNode(self.nodes[id])
            #     self.nodes_assigned_to_executors[id] = id
            #     print("Thread join executor found: ", id, " ", self.nodes[id].label)
            #     self.executorUpdate(id, id)

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

        print("Executors: ")
        for e in self.node_executors:
            print("Executor: ", e, " ", self.node_executors[e].execType)
            print("Nodes: ")
            for n in self.node_executors[e].nodes:
                print("\t", n, " ", self.node_executors[e].nodes[n].label)
            print("\n")

    def execute(self):
        pass

if __name__ == "__main__":
    # Get the model file as an argument
    model_file = sys.argv[1]
    # Load the model
    amexe = AppMakerExecutor(model_file)
    # Execute the model
    amexe.execute()

print("AppMakerExecutor loaded")