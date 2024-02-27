from appmakerExecutor import AppMakerExecutor

if __name__ == "__main__":
    """
    This script initializes and runs the AppMakerExecutor.
    """

    amexe = AppMakerExecutor()
    print("AppMakerExecutor loaded")

    amexe.commlib_node.run_forever()