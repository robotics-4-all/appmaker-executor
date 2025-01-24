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

from appmaker_node import AppMakerNode # type: ignore # pylint: disable=import-error
from appmaker_node_executor import NodeExecutor # type: ignore # pylint: disable=import-error
from appmaker_storage import StorageHandler # type: ignore # pylint: disable=import-error

class AppMakerExecutor(CommlibNode):
    """
    The AppMakerExecutor class is responsible for executing the model received from the AppMaker.
    """

    def __init__(self, *args, **kwargs): # pylint: disable=unused-argument
        """
        Initializes the AppMakerExecutor object.

        This method sets up the necessary attributes and establishes a connection to the 
        MQTT broker.
        It also creates a subscriber to listen for messages on the 
        'locsys/app_executor/deploy' topic.

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
        self.feedback_topic = None
        self.uid = None
        self.conn_params = None
        self.storage = None

        if "conn_params" in kwargs:
            self.conn_params = kwargs["conn_params"]
        del kwargs['conn_params']

        if "feedback_topic" in kwargs:
            self.feedback_topic = kwargs["feedback_topic"].replace("/", ".")
        print("Feedback topic: ", self.feedback_topic)
        del kwargs['feedback_topic']
        if "uid" in kwargs:
            self.uid = kwargs["uid"]
        print("UID: ", self.uid)
        del kwargs['uid']

        super().__init__(
            connection_params=self.conn_params,
            heartbeats=False,
            workers_rpc=10,
            **kwargs
        )

        self.publisher = self.create_publisher(
            topic=self.feedback_topic,
        )
        self.run()

    def find_corresponding_thread_join(self, thread_split_id):
        """
        Finds the corresponding thread join node for a given thread split node.

        Args:
            thread_split_id (int): The ID of the thread split node.

        Returns:
            int: The ID of the corresponding thread join node.
        """
        # Find the corresponding thread join node
        neighbors = [self.nodes[thread_split_id].connections[x]['target'] \
            for x in self.nodes[thread_split_id].connections]
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
                _neighs = temp_neighbors.copy()
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
                    _neighs.remove(_n)
                    # Add its neighbors to the set
                    _neighs.update([self.nodes[_n].connections[x]['target'] \
                        for x in self.nodes[_n].connections])

                    print("\tCurrent neighbors: ", [self.nodes[x].count for x in _neighs])
                    break

                temp_neighbors = _neighs.copy()
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

    def executor_update(self, node_id, executor_id):
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
            thread_join_id = self.find_corresponding_thread_join(node_id)
            print("The corresponding thread join is: ", self.nodes[thread_join_id].count)
            self.nodes[node_id].nextJoin = thread_join_id
            # Add the node to the executor
            self.node_executors[executor_id].add_node(self.nodes[thread_join_id])
            print("Thread join added to executor: ", executor_id)
            self.nodes_assigned_to_executors[thread_join_id] = executor_id
            # Update the executor
            self.executor_update(thread_join_id, executor_id)
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
                    self.node_executors[executor_id].add_node(self.nodes[n])
                    print("Node", self.nodes[n].count, "added to executor: ", executor_id)
                    self.executor_update(n, executor_id)

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
        self.storage = StorageHandler(self.uid, model)

        # Load the model from the file
        nodes = model['nodes']
        edges = model['edges']
        brokers = self.store['storeBrokers']

        # import pprint
        # pprint.pprint(self.store["storeNodes"])

        # Create the nodes
        for n in nodes:
            n = AppMakerNode(n, self.publisher, self.storage, brokers)
            self.nodes[n.id] = n

        # Create the edges
        for e in edges:
            self.nodes[e['source']].add_connection(self.nodes[e['target']], e)

        # Find Start nodes
        print("Starting executor finder")
        for _id, node in self.nodes.items():
            print("\n\t## Checking node: ", node.count)
            if node.label == "Start":
                self.node_executors[_id] = NodeExecutor("start")
                self.node_executors[_id].set_starting_node(_id)
                self.node_executors[_id].add_node(node)

                # Find artificial delay
                self.node_executors[_id].artificial_delay = node.parameters[0]['value']
                for n in self.store["storeNodes"]:
                    if n["id"] == _id and "parameters" in n and "parameters" in n["parameters"] and\
                        len(n["parameters"]["parameters"]) > 0:
                        delay_in_param = n["parameters"]["parameters"][0]["value"]
                        if delay_in_param != self.node_executors[_id].artificial_delay:
                            self.node_executors[_id].artificial_delay = delay_in_param

                print("Artificial delay: ", self.node_executors[_id].artificial_delay)

                self.nodes_assigned_to_executors[_id] = _id
                print("Starting executor for: ", self.nodes[_id].count,\
                    "\n\n========================")
                self.executor_update(_id, _id)
                print("Executor updated for: ", self.nodes[_id].count,\
                    "\n\n========================")

            if self.nodes[_id].label == "Thread split":
                # Start an executor from its neighbors
                # Find neighbors ids of the node
                neighbors = self.nodes[_id].connections
                print("Thread split found: ", self.nodes[_id].count)
                for n in neighbors:
                    if n not in self.nodes_assigned_to_executors:
                        self.node_executors[n] = NodeExecutor("thread")
                        self.node_executors[n].set_starting_node(n)
                        self.node_executors[n].add_node(self.nodes[n])
                        self.nodes_assigned_to_executors[n] = n
                        print("Thread executor for: ", self.nodes[n].count, \
                            "\n\n========================")
                        self.executor_update(n, n)
                        print("Executor updated for: ", self.nodes[n].count, \
                            "\n\n========================")
                        # Adding the threads as executors to node
                        self.nodes[_id].executors[n] = self.node_executors[n]

        print("Executors: ")
        for e, executor in self.node_executors.items():
            print("Executor: ", e, " ", executor.exec_type)
            print("Nodes: ")
            for n in executor.nodes:
                print("\t", executor.nodes[n].label, " ", executor.nodes[n].count)
            print("\n")

        # Find the preempt nodes
        for _, node in self.nodes.items():
            if node.label == "Preempt":
                thread_parameter = node.parameters[0]['value']
                # Split the thread parameter by :
                thread_parameter = thread_parameter.split(":")
                # Trim the spaces and make them integers
                thread_parameter = [int(x.strip()) for x in thread_parameter]
                node_count = thread_parameter[0]
                thread_count = thread_parameter[1]
                # Find the node with count equal to node_count
                for n, _node in self.nodes.items():
                    if _node.count == node_count:
                        # Find the next node of the thread with count equal to thread_count
                        for c in _node.connections:
                            print(c, _node.connections[c])
                            if _node.connections[c]['sourceHandle'] == f"out_{thread_count}":
                                node.executor_to_preempt = self.node_executors[c]
                                print("Executor to preempt: ", c, " ", self.nodes[c].label)
                                break

    def execute(self):
        """
        Executes the program by publishing a "start" message, executing all start
        nodes in threads, waiting for all start nodes to finish, and then 
        publishing an "end" message.
        """
        self.storage.set_publisher(self.publisher)
        self.publisher.publish({
            "program": "start"
        })

        # Gather all start nodes and execute them in threads
        # pylint: disable=consider-using-dict-items
        start_executors = [self.node_executors[e] for e in self.node_executors \
            if self.node_executors[e].exec_type == "start"]
        print("Start executors: ", start_executors)
        for s in start_executors:
            s.execute_threaded()

        # Wait for all start nodes to finish
        while True:
            time.sleep(0.1)
            # pylint: disable=consider-using-dict-items
            if all([self.node_executors[e].finished for e in self.node_executors \
                if self.node_executors[e].exec_type == "start"]):
                break

        print("Execution finished")
        self.publisher.publish({
            "program": "end"
        })

        # Stop everything
        self.storage.stop()
