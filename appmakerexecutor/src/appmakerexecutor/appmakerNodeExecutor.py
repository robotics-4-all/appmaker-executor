import threading

class NodeExecutor:
    def __init__(self, execType):
        """
        Initialize the AppMakerNodeExecutor object.

        Parameters:
        - execType (str): The execution type of the executor.

        Attributes:
        - starting_node: The starting node of the executor.
        - is_preempted: A flag indicating whether the executor is preempted.
        - execType: The execution type of the executor.
        - nodes: A dictionary containing the nodes of the executor.
        - runner: The runner object for executing the nodes.
        - finished: A flag indicating whether the executor has finished.
        - to_preempt: A flag indicating whether the executor should be preempted.
        - is_preempted: A flag indicating whether the executor is preempted.
        """
        self.starting_node = None
        self.is_preempted = False
        self.execType = execType
        self.nodes = {}
        self.runner = None
        self.finished = False
        self.to_preempt = False
        self.is_preempted = False
        self.artificial_delay = 0

    def addNode(self, node):
            """
            Adds a node to the executor.

            Args:
                node: The node object to be added.

            Returns:
                None
            """
            self.nodes[node.id] = node

    def setStartingNode(self, node_id):
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

            This method starts the execution of the node executor by setting the is_preempted flag of all nodes to False,
            initializing the finished flag to False, and setting the starting node as the runner. It then enters a loop
            where it repeatedly calls the execute method of the current runner node until the runner becomes None or is not
            present in the nodes dictionary. Finally, it sets the finished flag to True.

            Returns:
                None
            """
            print("Executor: ", self.execType, " started")
            # Make the is_preempted flag of all nodes false
            for n in self.nodes:
                self.nodes[n].artificial_delay = self.artificial_delay
                self.nodes[n].is_preempted = False
            self.finished = False
            self.runner = self.starting_node
            while self.runner != None and self.runner in self.nodes:
                self.runner = self.nodes[self.runner].execute()
                print("Runner: ", self.runner)
            print("Executor: ", self.execType, " finished")
            self.finished = True

    def executeThreaded(self):
        """
        Executes the `execute` method in a separate thread.
        """
        threading.Thread(target=self.execute).start()

    def enforcePreemption(self):
            """
            Sets the is_preempted flag to True for the current node and all its child nodes.
            This indicates that the execution of the current node should be preempted and the next node should take over.
            """
            self.is_preempted = True
            for n in self.nodes:
                self.nodes[n].is_preempted = True
            # No need to finish this, the next node will take care of it
