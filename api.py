import requests
url = "https://api.coindesk.com/v1/bpi/historical/close.json"
params = {
    "start": "2025-11-25",
    "end":   "2025-11-30"
}
try:
    resp = requests.get(url, params=params, timeout=10)
    resp.raise_for_status()
    print(resp.json())
except Exception as e:
    print("Erreur:", e)
