import threading

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
