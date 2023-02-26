import time
import argparse
# import rapidjson
import sys
from datetime import datetime as dt

import influxdb_client
import numpy as np
from influxdb_client.client.write_api import WritePrecision
from pythonosc.udp_client import SimpleUDPClient
import paho.mqtt.client as mqtt

VALID_TYPES = ['acc', 'bvp', 'temp', 'gsr', 'tag']

# Create a numpy moving average buffer of 100 samples
acc_buffer_length = 10
acc_x_buffer = np.zeros(acc_buffer_length)
acc_y_buffer = np.zeros(acc_buffer_length)
acc_z_buffer = np.zeros(acc_buffer_length)

start_time = time.time()

client = influxdb_client.InfluxDBClient(
    url="http://localhost:8086/",
    token='RR74qUnA269MrhNwi_00wFN3qCGFFeqM8GzeTohn61bubo9jbIdhOFLS09BTpK10q3a9-rBeNf_RNl0Pot-k2g==',
    org="388f9a80a7c06f54"
)

write_api = client.write_api()
query_api = client.query_api()


def write_to_influx(sensor, device_uid, run_tag, timestamp, value, x=None, y=None, z=None):
    p = influxdb_client.Point(sensor)
    p.tag("device_id", device_uid)
    p.tag("run_tag", run_tag)
    p.time(timestamp, WritePrecision.MS)
    if value:
        p.field("reading", value)
    else:
        p.field("x", x)
        p.field("y", y)
        p.field("z", z)

    write_api.write(bucket="e4-bucket", org="388f9a80a7c06f54", record=p)
    print(p, flush=True)


def ingest_data():
    content_type = request.headers.get('Content-Type')
    osc_client = SimpleUDPClient(app.config.get('OSC_IP'), app.config.get('OSC_PORT'))
    if content_type == 'application/json':
        res = request.json
    else:
        res = json.loads(request.data)

    print(f'Incoming request >>>> {res}')
    if res.get('type') == 'temp':
        temperature_event(res.get('device'), res.get('run'), res.get('timestamp'), res.get('value'), osc_client)
    elif res.get('type') == 'bvp':
        bvp_event(res.get('device'), res.get('run'), res.get('timestamp'), res.get('value'), osc_client)
    elif res.get('type') == 'acc':
        accelerometer_event(res.get('device'), res.get('run'), res.get('timestamp'),
                            res.get('x'), res.get('y'), res.get('z'), osc_client)
    elif res.get('type') == 'gsr':
        gsr_event(res.get('device'), res.get('run'), res.get('timestamp'), res.get('value'), osc_client)
    elif res.get('type') == 'tag':
        tag_event(res.get('device'), res.get('run'), res.get('timestamp'), res.get('value'), osc_client)

    return '{ "result": "json loaded" }'


def convert_range(value, in_min, in_max, out_min=0.0, out_max=1.0):
    in_range = in_max - in_min
    out_range = out_max - out_min
    return (((value - in_min) * out_range) / in_range) + out_min


def is_recording():
    return args.record


def is_quiet_mode():
    return args.quiet


def accelerometer_event(device_uid, run_tag, timestamp, x, y, z, osc_client):
    dt = timestamp - start_time
    if not is_quiet_mode():
        print("acc ", device_uid, dt, x, y, z)

    # Convert values in the range -90.0 - 90.0 to 0.0 - 1.0
    x = convert_range(x, -90.0, 90.0)
    y = convert_range(y, -90.0, 90.0)
    z = convert_range(z, -90.0, 90.0)

    # Add the new value to the buffers
    acc_x_buffer[:-1] = acc_x_buffer[1:]
    acc_x_buffer[-1] = x
    acc_y_buffer[:-1] = acc_y_buffer[1:]
    acc_y_buffer[-1] = y
    acc_z_buffer[:-1] = acc_z_buffer[1:]
    acc_z_buffer[-1] = z

    # Calculate the moving average of the buffer
    average_x = np.mean(acc_x_buffer)
    average_y = np.mean(acc_y_buffer)
    average_z = np.mean(acc_z_buffer)

    print("/e4/acc/x", average_x, average_y, average_z)
    osc_client.send_message("/e4/acc/x", average_x)
    osc_client.send_message("/e4/acc/y", average_y)
    osc_client.send_message("/e4/acc/z", average_z)

    if is_recording():
        write_to_influx('acc', device_uid, run_tag, timestamp, None, average_x, average_y, average_z)


def bvp_event(device_uid, run_tag, timestamp, value, osc_client):
    dt = timestamp - start_time
    if not is_quiet_mode():
        print("bvp", device_uid, timestamp, value)

    # Convert values in the range -500.0 - 500.0 to 0.0 - 1.0
    bvp = convert_range(value, -80.0, 80.0)

    osc_client.send_message("/e4/bvp", bvp)

    if is_recording():
        write_to_influx('bvp', device_uid, run_tag, timestamp, bvp)  # or value?

def temperature_event(device_uid, run_tag, timestamp, value, osc_client):
    dt = timestamp - start_time
    if not is_quiet_mode():
        print("temp", device_uid, timestamp, value)

    # Convert values in the range 25 - 36 to 0.0 - 1.0
    temp = convert_range(value, 25.0, 36.0)

    osc_client.send_message("/e4/temp", temp)

    if is_recording():
        write_to_influx('temp', device_uid, run_tag, timestamp, temp)


def gsr_event(device_uid, run_tag, timestamp, value, osc_client):
    dt = timestamp - start_time
    if not is_quiet_mode():
        print("gsr", device_uid, timestamp, value)

    # Convert values in the range 0.03 - 0.12 to 0.0 - 1.0
    gsr = convert_range(value, 0.06, 0.08)

    osc_client.send_message("/e4/gsr", gsr)

    if is_recording():
        write_to_influx('gsr', device_uid, run_tag, timestamp, gsr)


def tag_event(device_uid, run_tag, timestamp, value, osc_client):
    dt = timestamp - start_time
    if not is_quiet_mode():
        print("tag", device_uid, timestamp, value)

    osc_client.send_message("/e4/tag", value)

    if is_recording():
        write_to_influx('tag', device_uid, run_tag, timestamp, value)

def on_subscribe(client, userdata, mid, qos,tmp=None):
    if isinstance(qos, list):
        qos_msg = str(qos[0])
    else:
        qos_msg = f"and granted QoS {qos[0]}"
    print(dt.now().strftime("%H:%M:%S.%f")[:-2] + " Subscribed " + qos_msg)

def on_mqtt_message(client, userdata, message, tmp=None):
    decoded_msg = str(message.payload.decode("utf-8"))
    print("received message: ", decoded_msg)


def on_mqtt_connect(client, userdata, flags, rc, v5config=None):
    if rc == 0:
        client.connected_flag=True #set flag
        print("connected OK Returned code=",rc)
        #client.subscribe(topic)
    else:
        print("Bad connection Returned code= ",rc)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Forward E4 app / mqtt messages to OSC.')
    parser.add_argument('--osc-ip', type=str, help='OSC server IP address', default='127.0.0.1')
    parser.add_argument('--osc-port', type=int, help='OSC server port', default=8000)
    parser.add_argument('--record', type=str, help='Log E4 streams to file', default=None)
    parser.add_argument('--replay', type=str, help='Replays an existing log file', default=None)
    parser.add_argument('--type', type=str, help='Filters the event type, separated by commas (e.g. bvp, gsr)',
                        default=None)
    parser.add_argument('--quiet', action='store_true', help='Don\'t log out all events')

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

    if args.quiet:
        print_events = False

    if args.replay and args.record:
        print("Cannot record and replay at the same time.")
        sys.exit(0)
    # if args.replay:
    #     start_replay(args.replay, args.osc_ip, args.osc_port, types)

    if not args.replay:
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

        client.connect(broker_address,
                       port=1883,
                       clean_start=mqtt.MQTT_CLEAN_START_FIRST_ONLY,
                       properties=properties,
                       keepalive=60
                       )

        # while not client.connected_flag:  # wait in loop
        #     print("In wait loop")
        time.sleep(10)

        client.loop_start()
        client.subscribe("e4")

        print('>>>>>> stopping')
        client.loop_stop()
        client.disconnect()
