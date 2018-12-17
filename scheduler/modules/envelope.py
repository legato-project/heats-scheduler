from logging import basicConfig, info
from time import sleep
from typing import List

from kubernetes import client
from kubernetes.client import V1Pod

from modules.context import Context


class Envelope:

    def __init__(self, context: Context):
        self.scheduler_name = context.scheduler_name
        self.metrics_storage = context.metrics_storage
        self.api_k8s = client.CoreV1Api()
        basicConfig(filename='envelope.log', level=context.log_level, format='%(asctime)s - %(levelname)s - %(message)s')

    def list_running_pods(self) -> List[V1Pod]:
        """
        Get the pods running in the cluster
        :return: List of the running pods
        """
        pods = self.api_k8s.list_pod_for_all_namespaces(
            field_selector=("status.phase=Running,spec.schedulerName=%s" % self.scheduler_name)).items
        info("Found %d pods running" % len(pods))
        return pods

    def compute_envelopes(self, pod_name: str, envelopes: dict):
        tags = dict()
        fields = dict()
        measurement = "envelopes"
        tags['pod_name'] = pod_name
        fields['cpu'] = envelopes["cpu"]
        fields['mem'] = envelopes["mem"]
        fields['net'] = envelopes["net"]
        fields['disk'] = envelopes["disk"]
        return measurement, tags, fields


def main_loop_envelope(context: Context):
    env = Envelope(context)
    try:
        while True:
            pods = env.list_running_pods()
            for pod in pods:
                envelopes = env.metrics_storage.get_pod_metrics(pod.metadata.name)
                if envelopes:
                    measurement, tags, fields = env.compute_envelopes(pod.metadata.name, envelopes)
                    env.metrics_storage.write_point(measurement, tags, fields)
                    info("computing envelope : pod %s has envelopes cpu: %d, mem: %d, net: %d, disk: %d" % (pod.metadata.name, fields['cpu'], fields['mem'], fields['net'], fields['disk']))
            sleep(1)
    except KeyboardInterrupt:
        info("shutting down envelope")
        pass
