from logging import info
from math import sqrt

from influxdb import InfluxDBClient

from metrics.MetricsStorage import MetricsStorage


class InfluxDB(MetricsStorage):
    """
    Class used to get the metrics from influxDB
    """
    def __init__(self, args):
        self.influx_client = InfluxDBClient(args.influx_host, args.influx_port, args.influx_user, args.influx_pass, args.influx_database)

    def get_generation(self, pod_name: str) -> str:
        return "g"

    def query_last(self, query):
        req = list(self.influx_client.query(query))
        if req:
            if req[0][-1]['max']:
                return req[0][-1]['max']
            if req[0][-2]['max']:
                return req[0][-2]['max']
        return 0
    
    def get_limits(self, node_name: str) -> dict:
        res = self.influx_client.query('SELECT last(value) '
                                       'FROM k8s."default"."memory/node_capacity" '
                                       'WHERE nodename =~ /%s/;' % node_name)
        capacity_mem = 0
        if res:
            capacity_mem = list(res)[0][0]['last']
            
        res = self.influx_client.query('SELECT last(value) '
                                       'FROM k8s."default"."cpu/node_capacity" '
                                       'WHERE nodename =~ /%s/' % node_name)
        capacity_cpu = 0
        if res:
            capacity_cpu = list(res)[0][0]['last']
        
        return {"cpu": capacity_cpu, "memory": capacity_mem}
        
    def get_availability(self, node_name: str) -> dict:
        """
        Compute the availability of a node
        :param node_name: string representing the name of the node we want to compute the availability
        :return: a dictionnary with the availability of the node and the remaining ressource of Disk, Mem and Cpu under the fields : 'total', 'availableDisk','availableMem','availableCpu'
        """

        available_disk = self.query_last('SELECT max(value) FROM k8s."default"."filesystem/available"'
                                         'WHERE nodename =~ /%s/ and time > now() - 5m '
                                         'group by time(6s) fill(previous)' % node_name)

        usage_disk = self.query_last('SELECT max(value) FROM k8s."default"."filesystem/usage" '
                                     'WHERE nodename =~ /%s/ and time > now() - 5m '
                                     'group by time(6s) fill(previous)' % node_name)
        if usage_disk != 0 and available_disk != 0:
            ratio_disk = float(usage_disk) / float(usage_disk + available_disk)
        else:
            ratio_disk = 0

        res = self.influx_client.query('SELECT last(value) '
                                       'FROM k8s."default"."memory/node_capacity" '
                                       'WHERE nodename =~ /%s/;' % node_name)
        capacity_mem = 0
        if res:
            capacity_mem = list(res)[0][0]['last']

        usage_mem = self.query_last('SELECT max(value) FROM k8s."default"."memory/usage" '
                                    'WHERE nodename =~ /%s/ and time > now() - 5m '
                                    'group by time(6s) fill(previous)' % node_name)
        if usage_mem != 0 and capacity_mem != 0:
            ratio_mem = float(usage_mem) / float(capacity_mem)
        else:
            ratio_mem = 0

        res = self.influx_client.query('SELECT last(value) '
                                       'FROM k8s."default"."cpu/node_capacity" '
                                       'WHERE nodename =~ /%s/' % node_name)
        capacity_cpu = 0
        if res:
            capacity_cpu = list(res)[0][0]['last']

        usage_cpu = self.query_last('SELECT max(value) FROM k8s."default"."cpu/usage_rate" '
                                    'WHERE nodename =~ /%s/ and time > now() - 5m ' % node_name)
                                    #'group by time(1m) fill(previous)' % node_name)

        print("cpu_usage:")
        print(usage_cpu)
        print("cpu_capacity")
        print(capacity_cpu)
        if usage_cpu != 0 and capacity_cpu != 0:
            ratio_cpu = float(usage_cpu) / float(capacity_cpu)
        else:
            ratio_cpu = 0
        availability = sqrt(pow(ratio_cpu, 2) + pow(ratio_mem, 2) + pow(ratio_disk, 2))
        info("availability on node %s is %f" % (node_name, availability))
        return {'total': availability,
                'availableDisk': available_disk,
                'availableMem': capacity_mem - usage_mem,
                'availableCpu': capacity_cpu - usage_cpu}

    def get_envelopes(self, pod_name: str) -> dict:
        """
        Get all the envelopes of a given pod in the database
        :param pod_name: string
        :return: dictionary of the envelopes ('cpu', 'mem', 'disk', 'net')
        """
        query = 'SELECT last(*) FROM k8s."default"."envelopes" WHERE pod_name =~ /%s/' % pod_name.split('-')[0]
        result = list(self.influx_client.query(query))
        if result:
            envelope = dict()
            envelope['cpu'] = result[0][0]['last_cpu']
            envelope['mem'] = result[0][0]['last_mem']
            envelope['disk'] = result[0][0]['last_disk']
            envelope['net'] = result[0][0]['last_net']
            return envelope
        else:
            return {'cpu': 0, 'mem': 0, 'disk': 0, 'net': 0}

    def get_pod_metrics(self, pod_name: str) -> dict:
        """
        get the pod's metrics
        :return:
        """
        types_envelopes = {"cpu": "cpu/usage_rate", "net": "network/rx", "disk": "filesystem/usage", "mem": "memory/usage"}
        envelopes = {}
        for env_type in types_envelopes:
            query = 'SELECT percentile(value, 90) ' \
                    'FROM k8s."default"."%s" WHERE pod_name =~ /%s/;' % (types_envelopes[env_type], pod_name.split('-')[0])
            result = list(self.influx_client.query(query))
            if result:
                envelopes[env_type] = result[0][0]['percentile']
            else:
                return None
        return envelopes

    def write_measurement(self, measurement, nodetype, timestamp, value):
        json_body = [
        {
            "measurement": measurement,
            "tags": {
                "nodetype": nodetype,
            },
            "time": timestamp,
            "fields": {
                "value": value
            }
        }
        ]
        self.influx_client.write_points(json_body)

    def write_point(self, measurement: str, tags: dict, fields: dict):
        """
        Insert the pod envelopes in the database
        :param measurement: string name of the pod we want to insert the envelopes in the database
        :param tags: dictionary with the name of the
        :param fields: dictionary with the different envelopes to insert in the database
        """
        json_body = [
            {
                "measurement": measurement,
                "tags": tags,
                "fields": fields,
            }
        ]
        self.influx_client.write_points(json_body)
