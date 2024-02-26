from appmakerExecutor import AppMakerExecutor

if __name__ == "__main__":
    amexe = AppMakerExecutor()
    print("AppMakerExecutor loaded")

    amexe.commlib_node.run_forever()