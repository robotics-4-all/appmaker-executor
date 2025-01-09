"""
This file contains the NodeExecutor class, which is responsible for managing and executing a
series of nodes in a specified order.
"""

import threading

class NodeExecutor:
    """
    NodeExecutor is a class responsible for managing and executing a series of nodes in a 
    specified order.

        starting_node (int): The ID of the starting node for the executor.
        is_preempted (bool): A flag indicating whether the executor is preempted.
        exec_type (str): The execution type of the executor.
        nodes (dict): A dictionary containing the nodes of the executor.
        runner (int): The ID of the current runner node.
        finished (bool): A flag indicating whether the executor has finished.
        to_preempt (bool): A flag indicating whether the executor should be preempted.
        artificial_delay (int): An artificial delay to be applied to each node.

    Methods:
        __init__(exec_type):
            Initializes the NodeExecutor object with the specified execution type.
        add_node(node):
        set_starting_node(node_id):
            Sets the starting node for the executor.
        execute():
        execute_threaded():
        enforce_preemption():
    """
    def __init__(self, exec_type):
        self.starting_node = None
        self.is_preempted = False
        self.exec_type = exec_type
        self.nodes = {}
        self.runner = None
        self.finished = False
        self.to_preempt = False
        self.is_preempted = False
        self.artificial_delay = 0

    def add_node(self, node):
        """
        Adds a node to the executor.

        Args:
            node: The node object to be added.

        Returns:
            None
        """
        self.nodes[node.id] = node

    def set_starting_node(self, node_id):
        """
        Set the starting node for the executor.

        Args:
            node_id (int): The ID of the starting node.

        Returns:
            None
        """
        self.starting_node = node_id

    def execute(self):
        """
        Executes the node executor.

        This method starts the execution of the node executor by setting the is_preempted flag of 
        all nodes to False, initializing the finished flag to False, and setting the starting node 
        as the runner. It then enters a loop where it repeatedly calls the execute method of the 
        current runner node until the runner becomes None or is not present in the nodes dictionary.
        Finally, it sets the finished flag to True.

        Returns:
            None
        """
        print("Executor: ", self.exec_type, " started")
        # Make the is_preempted flag of all nodes false
        for _, node in self.nodes.items():
            node.artificial_delay = self.artificial_delay
            node.is_preempted = False
        self.finished = False
        self.runner = self.starting_node
        while self.runner is not None and self.runner in self.nodes:
            self.runner = self.nodes[self.runner].execute()
            print("Runner: ", self.runner)
        print("Executor: ", self.exec_type, " finished")
        self.finished = True

    def execute_threaded(self):
        """
        Executes the `execute` method in a separate thread.
        """
        threading.Thread(target=self.execute).start()

    def enforce_preemption(self):
        """
        Sets the is_preempted flag to True for the current node and all its child nodes.
        This indicates that the execution of the current node should be preempted and the 
        next node should take over.
        """
        self.is_preempted = True
        for _, node in self.nodes.items():
            node.is_preempted = True
        # No need to finish this, the next node will take care of it
