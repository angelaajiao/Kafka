from threading import Thread
from producer import Producer
from visualizer import Visualizer

if __name__ == '__main__':
    producer_thread = Thread(target=Producer().run)
    producer_thread.start()


    visualizer = Visualizer()
    visualizer_thread = Thread(target=visualizer.run)
    visualizer_thread.start()

    producer_thread.join()
    visualizer_thread.join()
