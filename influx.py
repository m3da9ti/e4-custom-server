import influxdb_client
from influxdb_client.client.write_api import WritePrecision


class Influx:
    def __init__(self, url, token, org, bucket='e4-bucket'):
        self.client = influxdb_client.InfluxDBClient(url, token, org)
        self.org = org
        self.bucket = bucket
        self.write_api = self.client.write_api()
        self.query_api = self.client.query_api()

    def write_to_influx(self, sensor, device_uid, run_tag, timestamp, value, x=None, y=None, z=None):
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

        self.write_api.write(bucket=self.bucket, org=self.org, record=p)
        print(p, flush=True)
