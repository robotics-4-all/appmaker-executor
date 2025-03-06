"""
File that initializes an Node of the AppMaker DSL.
"""

import time
import random

class AppMakerNode:
    """
    Node class represents a node in a system that can execute various actions based on its label 
    and parameters.
    Attributes:
        data (dict): The data associated with the node.
        publisher (object): The publisher used to publish messages.
        storage_handler (object): The storage handler used to manage variables and actions.
        id (str): The unique identifier of the node.
        label (str): The label of the node.
        toolbox (str): The toolbox associated with the node.
        count (int): The count of the node.
        parameters (list): The parameters of the node.
        connections (dict): The connections to other nodes.
        connection_list (list): The list of connections.
        brokers (list): The list of brokers.
        is_preempted (bool): Indicates if the node is preempted.
        executors (dict): The executors for thread split.
        next_join (object): The next join node for thread join.
        executor_to_preempt (object): The executor to preempt.
        artificial_delay (int): The artificial delay.
    Methods:
        __init__(self, data, publisher=None, storage_handler=None, brokers=[]):
            Initializes the Node with the given data, publisher, storage_handler, and brokers.
        addConnection(self, node, connection):
        publish(self, message):
        on_message(self, message):
            Handles the received message.
        execute(self):
        executeLog(self):
        executeSetVariable(self):
        executeCondition(self):
        executeRandom(self):
        executeThreadSplit(self):
        executePreempt(self):
        executeDelay(self):
        executeGeneral(self):
        printNode(self):
            Prints information about the node, including its ID, label, count, parameters, 
            and connections.
    """
    def __init__(self, data, publisher=None, storage_handler=None, brokers = None, stop_publisher=None):
        print("\n\n", data)
        self.data = data
        self.publisher = publisher
        self.storage_handler = storage_handler
        self.id = data['id']
        self.label = data['data']['label']
        self.toolbox = data['data']['toolbox']
        self.count = data['data']['count']
        self.parameters = data['data']['parameters'] if 'parameters' in data['data'] else []
        self.connections = {}
        self.connection_list = []
        self.brokers = brokers
        self.action_variable = None
        self.is_preempted = False
        # In case of thread split, we need to keep the executors
        self.executors = {}
        # In case of thread join, we need to keep the next join node
        self.next_join = None
        # In case of preempt, we need to keep the executor to kill
        self.executor_to_preempt = None
        self.artificial_delay = 0
        self.stop_publisher = stop_publisher

        # pprint.pprint(data)

    def add_connection(self, node, connection):
        """
        Adds a connection to the node.

        Args:
            node: The node to connect to.
            connection: The connection details.

        Returns:
            None
        """
        self.connections[node.id] = connection
        self.connection_list.append(connection)

    def publish(self, message):
        """
        Publishes a message using the publisher associated with this node.

        Args:
            message (str): The message to be published.

        Returns:
            None
        """
        print("Publishing message to UI", message)
        if self.publisher is not None:
            self.publisher.publish({
                "node_id": self.id,
                "message": message,
                "label": self.label,
                "timestamp": time.time(),
            })

    def publish_stop(self, message):
        """
        Publishes a message using the publisher associated with this node.

        Args:
            message (str): The message to be published.

        Returns:
            None
        """
        print("!! Publishing stop message")
        if self.stop_publisher is not None:
            self.stop_publisher.publish({
                "node_id": self.id,
                "message": message,
                "label": self.label,
            })

    def on_message(self, message):
        """
        Handles incoming messages and processes them.

        Args:
            message (str): The message received to be processed.

        Prints the received message and, if `self.actionVariable` is set, stores the message
        using `self.storage_handler`.

        """
        print("Received message: ", message)
        if self.action_variable:
            self.storage_handler.set(self.action_variable, message)

    def execute(self):
        """
        Executes the logic of the current node and returns the next node to be executed.

        Returns:
            next_node: The next node to be executed.
        """
        next_node = None
        if self.is_preempted:
            print("Node: ", self.id, " ", self.label, " is preempted")
            return None

        self.publish("start")
        if self.label == "Condition":
            next_node = self.execute_condition()
        elif self.label == "Random selection":
            next_node = self.execute_random()
        elif self.label == "End":
            time.sleep(1) # Delay to catch the websocket messages in Locsys
            next_node = None
        elif self.label == "Thread split":
            next_node = self.execute_thread_split()
        elif self.label == "Preempt":
            next_node = self.execute_preempt()
        elif self.label == "Delay":
            next_node = self.execute_delay()
        elif self.label == "Create variable" or self.label == "Set variable" or self.label == "Create List":
            next_node = self.execute_set_variable()
        elif self.label == "List operation" or self.label == "Manage list":
            next_node = self.execute_list_operation()
        elif self.label == "Log":
            next_node = self.execute_log()
        elif self.label == "Start Simulation":
            next_node = self.start_simulation()
        elif self.label == "Stop Simulation":
            next_node = self.stop_simulation()
        elif self.label == "Deploy GoalDSL model":
            next_node = self.deploy_goaldsl()
        elif self.label == "Stop GoalDSL model":
            next_node = self.stop_goaldsl()
        else: # All other nodes
            next_node = self.execute_general()

        # Handle actions
        if 'action' in self.data['data']:
            broker_id = None
            operation = None
            action = self.data['data']['action']

            # Subscriber
            if action['type'] == 'subscribe':
                # NOTE: Commenting out since we assume only redis here
                # # Find the broker id:
                # for p in self.data['data']['parameters']:
                #     if p['id'] == 'broker':
                #         print("Broker", p)
                #         broker_id = p['value']
                #         break
                # # Get broker
                # correct_broker = None
                # for b in self.brokers:
                #     if b["id"] == broker_id:
                #         correct_broker = b
                #         break
                correct_broker = None
                # Find the operation. Start or stop?
                print(">> Parameters: ", self.data['data']['parameters'])
                for p in self.data['data']['parameters']:
                    if p['id'] == 'operation':
                        operation = p['value']
                        break

                self.action_variable = action['storage']

                print("The correct broker is: ", correct_broker)
                if operation == "start":
                    print("Attempting to start subscriber")
                    self.storage_handler.start_subscriber(
                        action,
                        correct_broker,
                        self.on_message,
                    )
                    print(">> Subscribing to: ", action['topic'])
                elif operation == "stop":
                    self.storage_handler.stop_subscriber(
                        action,
                        correct_broker,
                    )
                else:
                    print("Something went wrong with action: ", action, correct_broker, operation)

            elif action['type'] == "publish":
                for p in self.data['data']['parameters']:
                    if p['id'] == 'broker':
                        broker_id = p['value']
                        break
                # Get broker
                correct_broker = None
                for b in self.brokers:
                    if b["id"] == broker_id:
                        correct_broker = b
                        break

                self.storage_handler.action_publish(
                    action,
                    correct_broker,
                    self.data['data']['parameters'],
                )

            elif action['type'] == "rpc":
                for p in self.data['data']['parameters']:
                    if p['id'] == 'broker':
                        broker_id = p['value']
                        break
                # Get broker
                correct_broker = None
                for b in self.brokers:
                    if b["id"] == broker_id:
                        correct_broker = b
                        break

                response = self.storage_handler.action_rpc_call(
                    action,
                    correct_broker,
                    self.data['data']['parameters'],
                )

                if 'storage' in action:
                    self.action_variable = action['storage']
                    if self.action_variable:
                        self.storage_handler.set(self.action_variable, response)

            elif action['type'] == "action":
                for p in self.data['data']['parameters']:
                    if p['id'] == 'broker':
                        broker_id = p['value']
                        break
                # Get broker
                correct_broker = None
                for b in self.brokers:
                    if b["id"] == broker_id:
                        correct_broker = b
                        break

                response = self.storage_handler.action_action_call(
                    action,
                    correct_broker,
                    self.data['data']['parameters'],
                )

                if 'storage' in action:
                    self.action_variable = action['storage']
                    if self.action_variable:
                        self.storage_handler.set(self.action_variable, response)

        # articifial delay
        time.sleep(self.storage_handler.evaluate(self.artificial_delay))

        print("Next node is ", next_node)
        self.publish("end")
        return next_node

    def find_proper_output_index_in_connections(self, ind):
        """
        Finds the proper index in the connections list.

        Args:
            ind (int): The index to find.

        Returns:
            int: The index in the connections list.
        """
        for i, c in enumerate(self.connection_list):
            if c['sourceHandle'] == f"out_{ind}":
                return i
        return -1

    def start_simulation(self):
        """
        Starts the simulation process.

        This method performs the following actions:
        1. Logs the current parameters.
        2. Retrieves the model from the parameters and initiates the simulation using the 
            storage handler.
        3. Generates a timestamp of the current time.
        4. Publishes a message indicating that the simulation has started, along with the 
            timestamp and node count.

        Returns:
            None
        """
        print("Starting simulation")
        model = self.parameters[0]['value']
        # Stop simulation before starting a new one
        self.storage_handler.reset_simulation()
        # Start the new one
        self.storage_handler.start_simulation(model)
        timestamp = time.strftime("%H:%M:%S", time.localtime())
        self.publish({
            "message": "Simulation started", 
            "timestamp": timestamp,
            "node_count": self.count,
        })
        time.sleep(3)
        return list(self.connections.keys())[0]

    def stop_simulation(self):
        """
        Starts the simulation process.

        This method performs the following actions:
        1. Logs the current parameters.
        2. Retrieves the model from the parameters and initiates the simulation using the 
            storage handler.
        3. Generates a timestamp of the current time.
        4. Publishes a message indicating that the simulation has started, along with the 
            timestamp and node count.

        Returns:
            None
        """
        print("Stopping simulation")
        resp = self.storage_handler.reset_simulation()
        print("Simulation stopped: ", resp)
        timestamp = time.strftime("%H:%M:%S", time.localtime())
        self.publish({
            "message": "Simulation stopped", 
            "timestamp": timestamp,
            "node_count": self.count,
        })
        time.sleep(3)
        return list(self.connections.keys())[0]

    def deploy_goaldsl(self):
        """
        Deploys the goaldsl model.

        This method starts the goaldsl model using the first parameter's value from the parameters 
        list.
        It then deploys the model using the storage handler, publishes a message indicating that the 
        goaldsl model has started along with the current timestamp and node count, and finally 
        returns the first connection key.

        Returns:
            str: The first key from the connections dictionary.
        """

        print("Starting goaldsl model")
        model = self.parameters[0]['value']
        # Start the new one
        self.storage_handler.deploy_goaldsl(model)
        timestamp = time.strftime("%H:%M:%S", time.localtime())
        self.publish({
            "message": "Goaldsl started", 
            "timestamp": timestamp,
            "node_count": self.count,
        })
        time.sleep(1)
        return list(self.connections.keys())[0]

    def stop_goaldsl(self):
        """
        Stops the GoalDSL service and publishes a message with the status.

        This method performs the following steps:
        1. Prints a message indicating that GoalDSL is stopping.
        2. Calls the storage handler to stop GoalDSL and prints the response.
        3. Gets the current local time and formats it as HH:MM:SS.
        4. Publishes a message containing the stop status, timestamp, and node count.
        5. Waits for 3 seconds.
        6. Returns the first key from the connections dictionary.

        Returns:
            str: The first key from the connections dictionary.
        """
        print("Stopping goaldsl")
        resp = self.storage_handler.stop_goaldsl()
        print("Goaldsl stopped: ", resp)
        timestamp = time.strftime("%H:%M:%S", time.localtime())
        self.publish({
            "message": "Goaldsl stopped", 
            "timestamp": timestamp,
            "node_count": self.count,
        })
        time.sleep(3)
        return list(self.connections.keys())[0]

    def execute_log(self):
        """
        Executes the log operation.

        This method logs the message to the console and returns the key of the first connection.

        Returns:
            str: The key of the first connection.
        """
        print("Log: ", self.parameters)
        message = self.parameters[0]['value']
        message = self.storage_handler.replace_variables(message)
        print("Log: ", message)
        # Get current time in literal format
        timestamp = time.strftime("%H:%M:%S", time.localtime())
        self.publish({
            "message": message, 
            "timestamp": timestamp,
            "node_count": self.count,
        })
        return list(self.connections.keys())[0]

    def execute_set_variable(self):
        """
        Executes the set variable operation.
        
        Sets the value of a variable with the given name to the evaluated 
        value of the provided expression.
        
        Returns:
            str: The key of the first connection.
        """
        variable_name = self.parameters[0]['value']
        variable_value = self.parameters[1]['value']
        if self.parameters[0]['id'] == "List" and self.parameters[1]['value'] == '':
            variable_value = []
        evaluated = self.storage_handler.evaluate(variable_value)
        print("Setting variable: ", variable_name, " ", evaluated)
        self.storage_handler.set(variable_name, evaluated)
        return list(self.connections.keys())[0]

    def execute_list_operation(self):
        try:
            storedList = self.storage_handler.get(self.parameters[0]['value'])
            
            if self.parameters[1]['value'] == "Pop":
                storedList.pop()
                self.storage_handler.set(self.parameters[0]['value'], storedList)
            elif self.parameters[1]['value'] == "Sort Ascending":
                storedList.sort()
                self.storage_handler.set(self.parameters[0]['value'], storedList)
            elif self.parameters[1]['value'] == "Sort Descending":
                storedList.sort(reverse=True)
                self.storage_handler.set(self.parameters[0]['value'], storedList)
            elif self.parameters[1]['value'] == "Push":
                evaluated = self.storage_handler.evaluate(self.parameters[2]['value'])
                storedList.append(evaluated)
                self.storage_handler.set(self.parameters[0]['value'], storedList)
            elif self.parameters[1]['value'] == "Delete by index":
                evaluatedIndex = self.storage_handler.evaluate(self.parameters[3]['value'])
                storedList.pop(evaluatedIndex)
                self.storage_handler.set(self.parameters[0]['value'], storedList)
            elif self.parameters[1]['value'] == "Delete All":
                evaluated = self.storage_handler.evaluate(self.parameters[2]['value'])
                storedList.clear()
                self.storage_handler.set(self.parameters[0]['value'], storedList)
            elif self.parameters[1]['value'] == "Delete by value":
                evaluatedIndex = self.storage_handler.evaluate(self.parameters[4]['value'])
                storedList.remove(evaluatedIndex)
                self.storage_handler.set(self.parameters[0]['value'], storedList)
            elif self.parameters[2]['value'] == "Average":
                meanvalue = sum(storedList)/len(storedList)
                variable_name = self.parameters[1]['value']
                self.storage_handler.set(variable_name, meanvalue)
            elif self.parameters[2]['value'] == "Max":
                maxvalue = max(storedList)
                variable_name = self.parameters[1]['value']
                self.storage_handler.set(variable_name, maxvalue)
            elif self.parameters[2]['value'] == "Min":
                minvalue = min(storedList)
                variable_name = self.parameters[1]['value']
                self.storage_handler.set(variable_name, minvalue)
            elif self.parameters[2]['value'] == "Standard Deviation":
                meanvalue = sum(storedList) / len(storedList)            
                variance = sum((x - meanvalue) ** 2 for x in storedList) / len(storedList)
                stddev = variance ** 0.5
                variable_name = self.parameters[1]['value']
                self.storage_handler.set(variable_name, stddev)
            elif self.parameters[2]['value'] == "Length":
                lenvalue = len(storedList)
                variable_name = self.parameters[1]['value']
                self.storage_handler.set(variable_name, lenvalue)  
            elif self.parameters[2]['value'] == "Includes":
                valuetosearch = self.storage_handler.evaluate(self.parameters[3]['value'])
                searchresult = valuetosearch in storedList
                variable_name = self.parameters[1]['value']
                self.storage_handler.set(variable_name, searchresult)
            elif self.parameters[2]['value'] == "Element count":
                valuetosearch = self.storage_handler.evaluate(self.parameters[4]['value'])
                searchresult = storedList.count(valuetosearch)
                variable_name = self.parameters[1]['value']
                self.storage_handler.set(variable_name, searchresult)
            elif self.parameters[2]['value'] == "Get element by index":
                evaluatedIndex = self.storage_handler.evaluate(self.parameters[5]['value'])
                searchresult = storedList[evaluatedIndex]
                variable_name = self.parameters[1]['value']
                self.storage_handler.set(variable_name, searchresult)
            elif self.parameters[2]['value'] == "Get index of element":
                valuetosearch = self.storage_handler.evaluate(self.parameters[6]['value'])
                searchresult = storedList.index(valuetosearch)
                variable_name = self.parameters[1]['value']
                self.storage_handler.set(variable_name, searchresult)
            return list(self.connections.keys())[0]
        
        except Exception as e:
            print(f"An error occurred: {e}")
            timestamp = time.strftime("%H:%M:%S", time.localtime())
            self.publish({
                "message": f"runtime_error: {e}",
                "node_count": self.count,
                "timestamp": timestamp,
            })
            self.publish("end") # Inform the UI that the executor has finished
            print("Published to UI")
            time.sleep(1)
            self.publish_stop(f"An error occurred: {e}")
            print("Internal publish")
            time.sleep(1)
            return None

    def execute_condition(self):
        """
        Executes the condition of the node and returns the next node to be executed.

        This method evaluates the conditions specified in the node's parameters and
        selects the next node
        based on the first condition that evaluates to True. If none of the conditions
        evaluate to True, the method returns None.

        Returns:
            str: The ID of the next node to be executed.
        """
        # Select one of the outputs at random
        print("Executing node: ", self.id, " ", self.label)
        next_node_index = 0
        for p in self.parameters:
            print(">>", p['id'], " ", p['value'])
            # Evaluate the condition
            result = False
            try:
                result = self.storage_handler.evaluate(str(p['value']))
                if isinstance(result, str):
                    result = False
                print("Result: ", result)
            except Exception as e: # pylint: disable=broad-except
                print("Error in evaluating the condition: ", e)
            if result:
                break
            next_node_index += 1
        print("Selected form condition: ", next_node_index)
        if next_node_index >= len(self.connection_list):
            next_node_index = len(self.connection_list) - 1 # The else condition
        real_output = self.find_proper_output_index_in_connections(next_node_index)
        if real_output == -1:
            print("!!!!!! ----->>>> Error in selecting the next node")
            return None
        print("Real output = ", real_output)
        return list(self.connections.keys())[real_output]

    def execute_random(self):
        """
        Executes the node by randomly selecting one of the outputs based on the
        probabilities assigned to each output.

        Returns:
            The selected output connection.
        """
        # Select one of the outputs at random
        print("Executing node: ", self.id, " ", self.label)
        # Gather all the parameters and evaluate them
        probabilities = [self.storage_handler.evaluate(x['value']) for x in self.parameters]
        prob_sum = sum(probabilities)
        random_prob = random.uniform(0, prob_sum)
        print(self.connection_list)
        print("Random probability: ", random_prob)
        for i, prob in enumerate(probabilities):
            if random_prob < prob:
                print("Selected: ", i)
                real_output = self.find_proper_output_index_in_connections(i)
                return self.connection_list[real_output]['target']
            random_prob -= prob
        print("Something went wrong, returning the last connection")
        return self.connection_list[-1]['target']

    def execute_thread_split(self):
        """
        Executes the node in a threaded manner.

        This method starts the executors threaded and waits for them to finish.
        It prints the node ID and label before executing the threads.
        """
        # We must start the executors threaded
        print("Executing node: ", self.id, " ", self.label)
        if self.executors:
            print("Executing threads")
            for _, executor in self.executors.items():
                executor.finished = False
                executor.execute_threaded()
            print("Waiting for threads to finish")
            while True:
                time.sleep(0.1)
                # pylint: disable=consider-using-dict-items
                if all([self.executors[e].finished for e in self.executors]):
                    print("Threads finished")
                    break
        return self.next_join

    def execute_preempt(self):
        """
        Executes the node in a preemptive manner.

        This method enforces preemption by sleeping for 1 second and then
        calling the `enforcePreemption` method of the `executor_to_preempt`
        object. It returns the key of the first connection in the `connections`
        dictionary.

        Returns:
            str: The key of the first connection in the `connections` dictionary.
        """
        print("Executing node: ", self.id, " ", self.label)
        self.executor_to_preempt.enforce_preemption()
        return list(self.connections.keys())[0]

    def execute_delay(self):
        """
        Executes the delay node by waiting for the specified delay time.

        Returns:
            str: The ID of the next connected node.
        """
        # Wait for the delay time
        print("Executing node: ", self.id, " ", self.label)
        print(self.parameters)
        if 'value' not in self.parameters[0]:
            print("Delay parameter not found")
            return None
        print("Delay parameter: ", self.parameters[0]['value'])
        delay = self.storage_handler.evaluate(self.parameters[0]['value'])
        print("Delay: ", delay)
        tt = 0
        while tt < float(delay) and not self.is_preempted:
            time.sleep(0.1)
            tt += 0.1
        return list(self.connections.keys())[0]

    def execute_general(self):
        """
        Executes the general node.

        This method prints the node ID and label, sleeps for 1 second, and returns 
        the key of the first connection.

        Returns:
            str: The key of the first connection.
        """
        print("Executing node: ", self.id, " ", self.label)
        print(list(self.connections.keys())[0])
        return list(self.connections.keys())[0]

    def print_node(self):
        """
        Print information about the node, including its ID, label, count, parameters, 
        and connections.
        """
        print("Node: ", self.id, " ", self.label, " ", self.count)
        print("Parameters: ")
        for p in self.parameters:
            print("\t", p['id'], " ", p['value'] if 'value' in p else "")
        print("Connections: ")
        for c in self.connections:
            print("\tto ", c)
