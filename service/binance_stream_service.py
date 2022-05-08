import sys , websocket , json , datetime

sys.path.append('')
sys.path.append('utils/')

from config import *
from kafka_utils import produce

# -------------------------------------------------------------------------------------

previous_minute = 0
init_previous_minute = True

# -------------------------------------------------------------------------------------

def on_message(ws , message):
    parsed = json.loads(message)
    
    global init_previous_minute
    global previous_minute
    current_minute = datetime.datetime.fromtimestamp(parsed['E']/1000).minute

    if init_previous_minute:
        previous_minute = current_minute
        init_previous_minute = False

    if previous_minute + BINANCE_INTERVAL >= 60:
        if previous_minute + BINANCE_INTERVAL - 60 == current_minute:
            print("received a message -> {}".format(message))
            produce(message)
            previous_minute = current_minute
    else:
        if previous_minute + BINANCE_INTERVAL == current_minute:
            print("received a message -> {}".format(message))
            produce(message)
            previous_minute = current_minute

# -------------------------------------------------------------------------------------

ws = websocket.WebSocketApp(BINANCE_API_URL + ":" + BINANCE_API_PORT + "/ws/" + BINANCE_STREAM_NAME , on_message=on_message)
ws.run_forever()

# -------------------------------------------------------------------------------------