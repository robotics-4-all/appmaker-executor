"""
This module provides a StorageHandler class for managing key-value storage.
"""

import re

class StorageHandler:
    """
    A class for managing key-value storage.
    """
    def __init__(self):
        self.storage = {}

    def get(self, key):
        """
        Retrieve the value associated with the given key.

        Args:
            key (str): The key to retrieve the value for.

        Returns:
            The value associated with the key, or None if the key does not exist.
        """
        return self.storage.get(key)
    
    def set(self, key, value):
        """
        Set the value for the given key.

        Args:
            key (str): The key to set the value for.
            value: The value to be stored.

        Returns:
            True if the value was successfully set.
        """
        self.storage[key] = value
        print("Value set: ", key, " ", value)
        return True
    
    def delete(self, key):
        """
        Delete the value associated with the given key.

        Args:
            key (str): The key to delete the value for.

        Returns:
            True if the value was successfully deleted, False if the key does not exist.
        """
        if key in self.storage:
            del self.storage[key]
            return True
        return False
    
    def evaluate(self, expression):
        """
        Evaluate an expression containing variables stored in the storage.

        Args:
            expression (str): The expression to evaluate.

        Returns:
            The result of the expression, or None if an error occurred during evaluation.
        """
        try:
            print("Evaluating expression: ", expression)
            pattern = r'\{([^}]*)\}'
            matches = re.findall(pattern, expression)
            for match in matches:
                variable_value = self.get(match)
                if variable_value is not None:
                    expression = expression.replace("{" + match + "}", str(variable_value))
            print("Evaluated expression: ", expression)
            return eval(expression)
        except Exception as e:
            print("Error during evaluation: ", e)
            return None