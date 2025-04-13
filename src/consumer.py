import json
import matplotlib.pyplot as plt
import matplotlib.animation as animation
from grafica import create_consumer, init_data_structures

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

        # Manejo de excepciones
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
        
        # Actualizamos las listas
        timestamps.append(data['timestamp'])
        prices.append(data['price_usd'])
        hash_rates.append(data['hash_rate_ths'])

        # Actualizar datos
        line_price.set_data(list(timestamps), list(prices))
        line_hash.set_data(list(timestamps), list(hash_rates))

        # Actualizamos las etiquetas del eje X (timestamps como hora)
        ax1.set_xticks(range(len(timestamps)))  # Cada punto de tiempo en el eje X
        ax1.set_xticklabels(timestamps, rotation=45, ha='right')  # Mostrar los timestamps como etiquetas

        ax1.set_xlim(0, len(prices))
        ax1.set_ylim(min(prices) * 0.98, max(prices) * 1.02)
        ax2.set_ylim(min(hash_rates) * 0.98, max(hash_rates) * 1.02)

        ax1.set_xlabel('Timestamps (Hora)')
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
 
ani = animation.FuncAnimation(fig, animate, interval=1000, cache_frame_data=False)
plt.title("Bitcoin Price & Hash Rate (Real-Time)")
plt.tight_layout()
plt.show()
