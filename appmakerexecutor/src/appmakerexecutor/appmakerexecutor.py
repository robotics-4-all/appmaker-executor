"""
This module contains the implementation of the AppMakerExecutor class, 
which is responsible for executing the model received from the AppMaker.

The AppMakerExecutor class initializes the necessary attributes, 
establishes a connection to the MQTT broker, and creates a subscriber 
to listen for messages on the 'locsys/app_executor/deploy' topic.

The class also provides methods for handling incoming messages, finding 
corresponding thread join nodes, updating the executor recursively, and 
loading the model into the executor.

Example usage:
    executor = AppMakerExecutor()
    executor.load_model(model)
    executor.execute()
"""

import time
from commlib.node import Node as CommlibNode
from commlib.transports.mqtt import ConnectionParameters

from appmakerNode import Node
from appmakerNodeExecutor import NodeExecutor
from appmakerStorage import StorageHandler

class AppMakerExecutor:
    """
    The AppMakerExecutor class is responsible for executing the model received from the AppMaker.
    """

    def __init__(self):
        """
        Initializes the AppMakerExecutor object.

        This method sets up the necessary attributes and establishes a connection to the MQTT broker.
        It also creates a subscriber to listen for messages on the 'locsys/app_executor/deploy' topic.

        Args:
            None

        Returns:
            None
        """
        self.nodes = {}
        self.store = {}
        self.node_executors = {}
        self.nodes_assigned_to_executors = {}
        self.publisher = None

        conn_params = ConnectionParameters(
            host="locsys.issel.ee.auth.gr",
            port=1883,
            username="r4a",
            password="r4a123$"
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
        """
        Handles incoming messages.

        Args:
            message (dict): The message received.

        Returns:
            None
        """
        print("Received model")
        print("Feedback on:", message['feedbackTopic'])
        self.publisher = self.commlib_node.create_publisher(topic=message['feedbackTopic'])
        self.load_model(message)
        self.execute()

    def findCoorespondingThreadJoin(self, thread_split_id):
        """
        Finds the corresponding thread join node for a given thread split node.

        Args:
            thread_split_id (int): The ID of the thread split node.

        Returns:
            int: The ID of the corresponding thread join node.
        """
        # Find the corresponding thread join node
        neighbors = [self.nodes[thread_split_id].connections[x]['target'] for x in self.nodes[thread_split_id].connections]
        print("\nInitial thread neighbors: ", [self.nodes[x].count for x in neighbors])
        final_joins = set()
        # Interate for each neighbor until the corresponding thread join is found
        for n in neighbors:
            print("Initial thread neighbor for check: ", self.nodes[n].count)
            joins_found = 0
            splits_found = 1
            temp_neighbors = set([n])
            visited = set()
            all_visited = False
            while joins_found < splits_found and not all_visited:
                all_visited = True
                for _n in temp_neighbors:
                    visited.add(_n)
                    print("Current node: ", self.nodes[_n].count)
                    if self.nodes[_n].label == "Thread join":
                        print("Thread join found: ", self.nodes[_n].count)
                        joins_found += 1
                        print("Joins found: ", joins_found)
                    elif self.nodes[_n].label == "Thread split":
                        splits_found += 1
                        print("Splits found: ", splits_found)
                
                    if joins_found == splits_found:
                        print("We are done, the correct join is: ", self.nodes[_n].count)
                        final_joins.add(_n)
                        break

                    # Remove the node from the neighbors
                    temp_neighbors.remove(_n)
                    # Add its neighbors to the set
                    temp_neighbors.update([self.nodes[_n].connections[x]['target'] for x in self.nodes[_n].connections])

                    print("\tCurrent neighbors: ", [self.nodes[x].count for x in temp_neighbors])
                    break

                # Check if all the neighbors were visited
                for _n in temp_neighbors:
                    if _n not in visited:
                        all_visited = False
                        break

        print("Final joins: ", [self.nodes[x].count for x in final_joins], '\n')
        if len(final_joins) == 1:
            return final_joins.pop()
        else:
            print("Error: More than one thread join found")
            
        return None

    def executorUpdate(self, node_id, executor_id):
        """
        Updates the executor by recursively traversing the graph starting from the given node.

        Args:
            node_id (int): The ID of the current node.
            executor_id (int): The ID of the executor.

        Returns:
            None
        """
        print("Executor update for node ", self.nodes[node_id].count, ":", executor_id)
        # Check if the node is a terminator (unless the executor starts from the terminator)
        if self.nodes[node_id].label == "End": 
            print("The end was found: ", self.nodes[node_id].count)
            return
        elif self.nodes[node_id].label == "Thread split":
            print("A thread split was found: ", self.nodes[node_id].count)
            # Find the corresponding thread join node
            thread_join_id = self.findCoorespondingThreadJoin(node_id)
            print("The corresponding thread join is: ", self.nodes[thread_join_id].count)   
            self.nodes[node_id].nextJoin = thread_join_id
            # Add the node to the executor
            self.node_executors[executor_id].addNode(self.nodes[thread_join_id])
            print("Thread join added to executor: ", executor_id)
            self.nodes_assigned_to_executors[thread_join_id] = executor_id
            # Update the executor
            self.executorUpdate(thread_join_id, executor_id)
        else:
            print("Not stopping! Going for neighbors of ", self.nodes[node_id].count)
            # Find neighbors ids of the node
            neighbors = self.nodes[node_id].connections
            # Assign the neighbors to the executor
            for n in neighbors:
                print("Neighbor: ", self.nodes[n].count)
                if n not in self.nodes_assigned_to_executors:
                    print("Neighbor not assigned to executor yet, going for it")
                    self.nodes_assigned_to_executors[n] = executor_id
                    self.node_executors[executor_id].addNode(self.nodes[n])
                    print("Node", self.nodes[n].count, "added to executor: ", executor_id)
                    self.executorUpdate(n, executor_id)
            
    def load_model(self, model):
        """
        Loads the model into the executor.

        Args:
            model (dict): The model containing nodes and edges.

        Returns:
            None
        """
        self.nodes = {}
        self.store = model['store']
        self.node_executors = {}
        self.nodes_assigned_to_executors = {}
        self.storage = StorageHandler()
        
        # Load the model from the file
        nodes = model['nodes']
        edges = model['edges']
        brokers = self.store['storeBrokers']

        # Create the nodes
        for n in nodes:
            n = Node(n, self.publisher, self.storage, brokers)
            self.nodes[n.id] = n

        # Create the edges
        for e in edges:
            self.nodes[e['source']].addConnection(self.nodes[e['target']], e)

        # Find Start nodes
        print("Starting executor finder")
        for id in self.nodes:
            print("\n\t## Checking node: ", self.nodes[id].count)
            if self.nodes[id].label == "Start":
                self.node_executors[id] = NodeExecutor("start")
                self.node_executors[id].setStartingNode(id)
                self.node_executors[id].addNode(self.nodes[id])
                self.nodes_assigned_to_executors[id] = id
                print("Starting executor for: ", self.nodes[id].count, "\n\n========================")
                self.executorUpdate(id, id)
                print("Executor updated for: ", self.nodes[id].count, "\n\n========================")

            if self.nodes[id].label == "Thread split":
                # Start an executor from its neighbors
                # Find neighbors ids of the node
                neighbors = self.nodes[id].connections
                print("Thread split found: ", self.nodes[id].count) 
                for n in neighbors:
                    if n not in self.nodes_assigned_to_executors:
                        self.node_executors[n] = NodeExecutor("thread")
                        self.node_executors[n].setStartingNode(n)
                        self.node_executors[n].addNode(self.nodes[n])
                        self.nodes_assigned_to_executors[n] = n
                        print("Thread executor for: ", self.nodes[n].count, "\n\n========================")
                        self.executorUpdate(n, n)
                        print("Executor updated for: ", self.nodes[n].count, "\n\n========================")
                        # Adding the threads as executors to node
                        self.nodes[id].executors[n] = self.node_executors[n]

        print("Executors: ")
        for e in self.node_executors:
            print("Executor: ", e, " ", self.node_executors[e].execType)
            print("Nodes: ")
            for n in self.node_executors[e].nodes:
                print("\t", self.node_executors[e].nodes[n].label, " ", self.node_executors[e].nodes[n].count)
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
            """
            Executes the program by publishing a "start" message, executing all start nodes in threads,
            waiting for all start nodes to finish, and then publishing an "end" message.
            """
            self.storage.setPublisher(self.publisher)

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

            # Stop everything
            self.storage.stop()
