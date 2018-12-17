from logging import info
from math import sqrt, pow

from metrics.MetricsStorage import MetricsStorage

even_envelope = {'cpu': 1500, 'mem': 1500000000, 'disk': 150000, 'net': 15500}
odd_envelope = {'cpu': 500, 'mem': 500000000, 'disk': 50000, 'net': 5500}
five_envelope = {'cpu': 1000, 'mem': 1000000000, 'disk': 100000, 'net': 10500}
null_envelope = {'cpu': 0, 'mem': 0, 'disk': 0, 'net': 0}


class InfluxTest(MetricsStorage):
    """
    Class used to get the metrics from fixed values
    """
    def get_generation(self, pod_name: str) -> str:
        """
        Find the generation where the pod should be according to the number of envelopes already computed on it
        :param pod_name:
        :return: a string representing the generation of the pod : 'nursery', 'young' or 'old'
        """
        if pod_name.split('-')[0] == 'nginx':
            return 'nursery'
        elif pod_name.split('-')[0] == 'nginx2':
            return 'young'
        elif pod_name.split('-')[0] == 'nginx4':
            return 'old'
        else:
            return None

    def get_availability(self, node_name: str) -> dict:
        """
        Compute the availability of a node
        :param node_name: string representing the name of the node we want to compute the availability
        :return: a dictionnary with the availability of the node and the remaining ressource of Disk, Mem and Cpu under the fields : 'availability', 'availableDisk','availableMem','availableCpu'
        """
        available_disk = 12000000000
        usage_disk = 4000000000
        ratio_disk = float(usage_disk) / float(usage_disk + available_disk)  # 0.25

        capacity_mem = 2000000000
        usage_mem = 1800000000
        ratio_mem = float(usage_mem) / float(capacity_mem)  # 0.9

        capacity_cpu = 2000
        usage_cpu = 200
        ratio_cpu = float(usage_cpu) / float(capacity_cpu)  # 0.1
        availability = sqrt(pow(ratio_cpu, 2) + pow(ratio_mem, 2) + pow(ratio_disk, 2))  # 0.8825
        info("availability on node %s is %f" % (node_name, availability))
        return {'availability': availability,
                'availableDisk': available_disk,
                'availableMem': capacity_mem - usage_mem,
                'availableCpu': capacity_cpu - usage_cpu}

    def get_envelopes(self, pod_name: str) -> dict:
        """
        Get all the envelopes of a given pod in the database
        :param pod_name: string
        :return: dictionary of the envelopes ('cpu', 'mem', 'disk', 'net')
        """
        pod_name = pod_name.split('-')[0]
        if pod_name == 'nginx5':
            return five_envelope
        elif pod_name != '' and pod_name[-1].isdigit() and int(pod_name[-1]) % 2 == 0:
            return even_envelope
        elif pod_name != '' and pod_name[-1].isdigit() and int(pod_name[-1]) % 2 == 1:
            return odd_envelope
        else:
            return null_envelope

    def get_pod_metrics(self, pod_name: str) -> dict:
        """
        get the pod's metrics
        :return: dictionary with the envelopes of the pod given in parameter
        """
        env = dict()
        if pod_name == '':
            env["cpu"] = 0
            env["net"] = 0
            env["disk"] = 0
            env["mem"] = 0
        else:
            env["cpu"] = 17000000
            env["net"] = 3000
            env["disk"] = 77000
            env["mem"] = 3200000
        return env

    def write_point(self, measurement: str, tags: dict, fields: dict):
        """
        Insert the pod envelopes in the database
        :param measurement: string name of the pod we want to insert the envelopes in the database
        :param tags: dictionary with the name of the
        :param fields: dictionary with the different envelopes to insert in the database
        """
        return True
