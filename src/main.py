from threading import Thread
from producer import Producer
from visualizer import Visualizer

if __name__ == '__main__':
    producer_thread = Thread(target=Producer().run)
    visualizer_thread = Thread(target=Visualizer().run)

    producer_thread.start()
    visualizer_thread.start()

    producer_thread.join()
    visualizer_thread.join()
