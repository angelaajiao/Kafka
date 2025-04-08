import json
import matplotlib.pyplot as plt
import matplotlib.animation as animation
from kafka import KafkaConsumer
from collections import deque

# Configuración
topic = 'btc-stream'
max_points = 10000

# Inicializar Kafka Consumer
consumer = KafkaConsumer(
    topic,
    bootstrap_servers='localhost:9092',
    group_id='btc-visualizer',
    auto_offset_reset='latest',
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

# Estructuras de datos
timestamps = deque(maxlen=max_points)
prices = deque(maxlen=max_points)
hash_rates = deque(maxlen=max_points)

# Gráfica con doble eje Y
fig, ax1 = plt.subplots()
ax2 = ax1.twinx()

line_price, = ax1.plot([], [], 'g-', label='BTC Price (USD)')
line_hash, = ax2.plot([], [], 'b-', label='Hash Rate (TH/s)')

def animate(i):
    try:
        msg = next(consumer)
        data = msg.value

        #Manejo de excepciones
        try:
            timestamp = data['timestamp']
            price = data['price_usd']
            hash_rate = data['hash_rate_ths']
        except KeyError as e:
            print(f"Error de clave: {e}")
            return
        except TypeError as e:
            print(f"Error de tipo al procesar el mensaje: {e}")
            return
        
        # Nos aseguramos de que los datos sean válidos
        if not(isinstance(price, (int, float)) and isinstance(hash_rate, (int, float))):
            print(f"Datos inválidos: price={price}, hash_rate={hash_rate}")
            return
        
        #actualizamos las listas
        timestamps.append(data['timestamp'])
        prices.append(data['price_usd'])
        hash_rates.append(data['hash_rate_ths'])

        # Actualizar datos
        line_price.set_data(range(len(prices)), prices)
        line_hash.set_data(range(len(hash_rates)), hash_rates)

        ax1.set_xlim(0, len(prices))
        ax1.set_ylim(min(prices) * 0.98, max(prices) * 1.02)
        ax2.set_ylim(min(hash_rates) * 0.98, max(hash_rates) * 1.02)

        ax1.set_xlabel('Timestamps (latest last)')
        ax1.set_ylabel('BTC Price (USD)', color='g')
        ax2.set_ylabel('Hash Rate (TH/s)', color='b')

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
 
ani = animation.FuncAnimation(fig, animate, interval=1000)
plt.title("Bitcoin Price & Hash Rate (Real-Time)")
plt.tight_layout()
plt.show()
