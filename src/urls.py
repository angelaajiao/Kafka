import requests
import json

class URLs:
    def get_bitcoin_price(self):
        try:
            response = requests.get('https://api.coinbase.com/v2/prices/spot?currency=')
            return float(response.json()['data']['amount'])
        except (requests.RequestException, json.JSONDecodeError, ValueError) as e:
            print(f"Error al obtener el precio de Bitcoin: {e}")
            return None

    def get_hash_rate(self):
        try:
            response = requests.get('https://api.blockchain.info/q/hashrate')
            return float(response.text) / 1000
        except (requests.RequestException, ValueError) as e:
            print(f"Error al obtener el hash rate: {e}")
            return None
