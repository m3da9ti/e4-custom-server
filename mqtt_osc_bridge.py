import argparse
import json
import sys
import time
import traceback
from datetime import datetime as dt

import paho.mqtt.client as mqtt
from pythonosc.udp_client import SimpleUDPClient

import events
from influx import Influx

VALID_TYPES = ['acc', 'bvp', 'temp', 'gsr', 'tag']

def on_subscribe(client, userdata, mid, qos, tmp=None):
    if isinstance(qos, list):
        qos_msg = str(qos[0])
    else:
        qos_msg = f"and granted QoS {qos[0]}"
    print(dt.now().strftime("%H:%M:%S.%f")[:-2] + " Subscribed " + qos_msg)


def on_mqtt_message(client, userdata, message, tmp=None):
    decoded_msg = str(message.payload.decode("utf-8"))
    print("received message: ", decoded_msg)
    res = json.loads(decoded_msg)
    osc_client = SimpleUDPClient(args.osc_ip, args.osc_port)

    if res.get('type') == 'temp':
        events.temperature_event(res, osc_client, influx)
    elif res.get('type') == 'bvp':
        events.bvp_event(res, osc_client, influx)
    elif res.get('type') == 'acc':
        events.accelerometer_event(res, osc_client, influx)
    elif res.get('type') == 'gsr':
        events.gsr_event(res, osc_client, influx)
    elif res.get('type') == 'tag':
        events.tag_event(res, osc_client, influx)


def on_mqtt_connect(client, userdata, flags, rc, v5config=None):
    if rc == 0:
        client.connected_flag = True
        print("connected OK Returned code=", rc)
    else:
        print("Bad connection Returned code= ", rc)


def signal_handler(sig, frame):
    print('You pressed Ctrl+C!')
    print('>>>>>> stopping')
    client.loop_stop()
    client.disconnect()
    sys.exit(0)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Forward E4 app / mqtt messages to OSC.')
    parser.add_argument('--osc-ip', type=str, help='OSC server IP address', default='127.0.0.1')
    parser.add_argument('--osc-port', type=int, help='OSC server port', default=8000)
    parser.add_argument('--record', action='store_true', help='Persist all events into Influx DB')
    parser.add_argument('--influx-url', type=str, help='Influx URL', default='http://localhost:8086/')
    parser.add_argument('--influx-token', type=str, help='Influx token', default='RR74qUnA269MrhNwi_00wFN3qCGFFeqM8GzeTohn61bubo9jbIdhOFLS09BTpK10q3a9-rBeNf_RNl0Pot-k2g==')
    parser.add_argument('--influx-org', type=str, help='Influx Org ID', default='388f9a80a7c06f54')
    parser.add_argument('--type', type=str, help='Filters the event type, separated by commas (e.g. bvp, gsr)',
                        default=None)
    parser.add_argument('--quiet', action='store_true', help='Don\'t log out all events')


    influx = None
    args = parser.parse_args()
    types = VALID_TYPES
    if args.type:
        types = args.type.split(',')
        types = [t.strip() for t in types]
        # Check if all types are valid
        for t in types:
            if t not in VALID_TYPES:
                print(f"Invalid event type: {t}")
                sys.exit(0)

    mqtt.Client.connected_flag = False
    broker_address = "192.168.1.4"  # make a param
    client = mqtt.Client("e4/osc replay", transport='tcp',
                         protocol=mqtt.MQTTv5)
    print("Connecting to broker ", broker_address)
    client.on_connect = on_mqtt_connect
    client.on_subscribe = on_subscribe
    client.on_message = on_mqtt_message

    from paho.mqtt.properties import Properties
    from paho.mqtt.packettypes import PacketTypes

    properties = Properties(PacketTypes.CONNECT)
    properties.SessionExpiryInterval = 30 * 60  # in seconds

    if args.record:
        print(f'Initializing Influx DB connection...')
        influx = Influx(args.influx_url, args.influx_token, args.influx_token)

    try:
        client.connect(broker_address,
                       port=1883,
                       clean_start=mqtt.MQTT_CLEAN_START_FIRST_ONLY,
                       properties=properties,
                       keepalive=60
                       )

        print(f'>>>> subscribing to the topic ...')
        client.subscribe("e4")

        # while not client.connected_flag:  # wait in loop
        #     print("In wait loop")
        time.sleep(10)

        print(f'>>>> starting the loop...')
        client.loop_forever()

        # print(f'>>>> setting the signal')
        # signal.signal(signal.SIGINT, signal_handler)
    except KeyboardInterrupt:
        print('Ctrl-C is pressed, stopping the loop...')
        client.loop_stop()
        client.disconnect()

    except Exception:
        traceback.print_exc()
