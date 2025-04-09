import matplotlib.pyplot as plt
import matplotlib.animation as animation
from collections import deque
from consumer import Consumer

class Visualizer:
    def __init__(self, max_points=10000):
        self.consumer = Consumer()
        self.timestamps = deque(maxlen=max_points)
        self.prices = deque(maxlen=max_points)
        self.hash_rates = deque(maxlen=max_points)

        self.fig, self.ax1 = plt.subplots()
        self.ax2 = self.ax1.twinx()
        self.line_price, = self.ax1.plot([], [], 'g-', label='BTC Price (USD)')
        self.line_hash, = self.ax2.plot([], [], 'b-', label='Hash Rate (TH/s)')

    def animate(self, i):
        data = self.consumer.get_data()
        if data is None:
            return

        try:
            timestamp = data['timestamp']
            price = data['price_usd']
            hash_rate = data['hash_rate_ths']
        except (KeyError, TypeError) as e:
            print(f"Error en datos: {e}")
            return

        self.timestamps.append(timestamp)
        self.prices.append(price)
        self.hash_rates.append(hash_rate)

        self.line_price.set_data(range(len(self.prices)), self.prices)
        self.line_hash.set_data(range(len(self.hash_rates)), self.hash_rates)

        self.ax1.set_xlim(0, len(self.prices))
        self.ax1.set_ylim(min(self.prices) * 0.98, max(self.prices) * 1.02)
        self.ax2.set_ylim(min(self.hash_rates) * 0.98, max(self.hash_rates) * 1.02)

        self.ax1.set_xlabel('Timestamps')
        self.ax1.set_ylabel('BTC Price (USD)', color='g')
        self.ax2.set_ylabel('Hash Rate (TH/s)', color='b')

    def run(self):
        ani = animation.FuncAnimation(self.fig, self.animate, interval=1000)
        plt.title("Bitcoin Price & Hash Rate (Real-Time)")
        plt.tight_layout()
        plt.show()
