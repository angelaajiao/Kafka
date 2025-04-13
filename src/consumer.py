import json
import matplotlib.pyplot as plt
import matplotlib.animation as animation
from grafica import create_consumer, init_data_structures
from datetime import datetime
import matplotlib.dates as mdates


# Configuración
topic = 'btc-stream'
max_points = 10000

# Inicializar Kafka Consumer
consumer = create_consumer(topic)
timestamps, prices, hash_rates = init_data_structures(max_points)

# Gráfica con doble eje Y
fig, ax1 = plt.subplots()
ax2 = ax1.twinx()

line_price, = ax1.plot([], [], 'g-', label='BTC Price (USD)')
line_hash, = ax2.plot([], [], 'b-', label='Hash Rate (TH/s)')

def animate(i):
    try:
        msg = next(consumer)
        data = msg.value

        try:
            timestamp_str = data['timestamp']
            price = data['price_usd']
            hash_rate = data['hash_rate_ths']
        except KeyError as e:
            print(f"Error de clave: {e}")
            return
        except TypeError as e:
            print(f"Error de tipo al procesar el mensaje: {e}")
            return

        if not (isinstance(price, (int, float)) and isinstance(hash_rate, (int, float))):
            print(f"Datos inválidos: price={price}, hash_rate={hash_rate}")
            return

        # Convertir timestamp a datetime
        try:
            timestamp_dt = datetime.fromisoformat(timestamp_str)
        except ValueError:
            print(f"Formato de timestamp inválido: {timestamp_str}")
            return

        timestamps.append(timestamp_dt)
        prices.append(price)
        hash_rates.append(hash_rate)

        ax1.set_xlim([min(timestamps), max(timestamps)])

        ax1.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))
        ax1.xaxis.set_major_locator(mdates.AutoDateLocator())
        fig.autofmt_xdate()
        
        line_price.set_data(timestamps, prices)
        line_hash.set_data(timestamps, hash_rates)
        ax1.set_xlim([min(timestamps), max(timestamps)])
        if len(prices) > 1 and len(hash_rates) > 1:
            price_margin = (max(prices) - min(prices)) * 0.1
            hash_margin = (max(hash_rates) - min(hash_rates)) * 0.1
            ax1.set_ylim(min(prices) - price_margin, max(prices) + price_margin)
            ax2.set_ylim(min(hash_rates) - hash_margin, max(hash_rates) + hash_margin)

        ax1.set_xlabel('Medidas (últimas a la derecha)')
        ax1.set_ylabel('BTC Price (USD)', color='g')
        ax2.set_ylabel('Hash Rate (TH/s)', color='b')
        ax1.legend(loc='upper left')
        ax2.legend(loc='upper right')


    except StopIteration:
        pass
    except json.JSONDecodeError as e:
        print(f"Error al decodificar JSON: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")
    except KeyboardInterrupt:
        print("Interrumpido por el usuario.")
        consumer.close()
        exit()
 
ani = animation.FuncAnimation(fig, animate, interval=1000, cache_frame_data=False)
plt.title("Bitcoin Price vs Hash Rate Evolution")
plt.tight_layout()
plt.show()
