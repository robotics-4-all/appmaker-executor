import time
from commlib.node import Node as CommlibNode
from commlib.transports.mqtt import ConnectionParameters

from appmakerNode import Node
from appmakerNodeExecutor import NodeExecutor

class AppMakerExecutor:
    def __init__(self):
        self.nodes = {}
        self.store = {}
        self.node_executors = {}
        self.nodes_assigned_to_executors = {}
        self.publisher = None

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
        print("Feedback on:", message['feedbackTopic'])
        self.publisher = self.commlib_node.create_publisher(topic=message['feedbackTopic'])
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
        self.nodes = {}
        self.store = {}
        self.node_executors = {}
        self.nodes_assigned_to_executors = {}
        
        # Load the model from the file
        nodes = model['nodes']
        edges = model['edges']
        # store = model['store']

        for n in nodes:
            n = Node(n, self.publisher)
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
        self.publisher.publish({
            "program": "start"
        })
        # Gather all start nodes and execute them in threads
        start_executors = [self.node_executors[e] for e in self.node_executors if self.node_executors[e].execType == "start"]
        for s in start_executors:
            s.executeThreaded()

        # Wait for all start nodes to finish
        while True:
            time.sleep(0.1)
            if all([self.node_executors[e].finished for e in self.node_executors if self.node_executors[e].execType == "start"]):
                break

        print("Execution finished")
        self.publisher.publish({
            "program": "end"
        })
