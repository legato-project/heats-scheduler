from datetime import datetime, timedelta
from logging import basicConfig, info, warning
from math import pow, sqrt
from time import sleep
from typing import List
import random
from kubernetes import client
from kubernetes.client import V1PodStatus, V1ObjectReference, V1Event, V1EventSource, V1Pod, V1PodSpec, V1Node, V1Binding, V1Container, V1DeleteOptions, V1ObjectMeta, V1ResourceRequirements
from kubernetes.client.rest import ApiException
from numpy import array, argmax
import traceback
from modules.context import Context
from metrics.ModelBuilder import ModelBuilder


class Scheduler:
    def __init__(self, context: Context):
        self.scheduler_name = context.scheduler_name
        self.api_k8s = client.CoreV1Api()
        self.rescheduling_time = context.rescheduling_time
        basicConfig(filename='scheduler.log', level=context.log_level, format='%(asctime)s - %(levelname)s - %(message)s')
        self.metrics_storage = context.metrics_storage
        self.model_builder = ModelBuilder()
        self.migration_minimum = 1

    def list_unscheduled_pods(self) -> List[V1Pod]:
        """
        Get all the unscheduled pods (pods in state "Pending")
        :return: List of Pods
        """
        pods = self.api_k8s.list_pod_for_all_namespaces(
            field_selector=("status.phase=Pending,spec.schedulerName=%s,spec.nodeName=" % self.scheduler_name)).items
        print("Found %d unscheduled pods" % len(pods))
        pods.sort(key=lambda x: x.metadata.creation_timestamp)
        return pods

    def list_scheduled_pods(self) -> List[V1Pod]:
        """
        Get all the unscheduled pods (pods in state "Pending")
        :return: List of Pods
        """
        pods = self.api_k8s.list_pod_for_all_namespaces(
            field_selector=("status.phase=Running,spec.schedulerName=%s" % self.scheduler_name)).items
        print("Found %d scheduled pods" % len(pods))
        return pods
        
    def list_scheduled_pods_node(self, node: str) -> List[V1Pod]:
        """
        Get all the unscheduled pods (pods in state "Pending")
        :return: List of Pods
        """
        pods = self.api_k8s.list_pod_for_all_namespaces(
            field_selector=("spec.nodeName=%s,spec.schedulerName=%s,status.phase!=Succeeded" % (node, self.scheduler_name))).items
        print("Found %d active pods scheduled in node %s" % (len(pods), node))
        return pods

    def node_is_ready(self, node: V1Node) -> bool:
        for condition in node.status.conditions:
            if condition.type == 'Ready' and condition.status == 'True':
                return True
        return False		
    
    def notify_binding(self, pod: V1Pod, node: V1Node):
        """
        Notify that a pod has been bind to a node.
        :param pod: type V1Pod
        :param node: type V1Node
        """
        timestamp = datetime.utcnow().isoformat("T") + "Z"

        event = V1Event(
            count=1,
            message=("Scheduled %s on %s" % (pod.metadata.name, node.metadata.name)),
            metadata=V1ObjectMeta(generate_name=pod.metadata.name + "-"),
            reason="Scheduled",
            last_timestamp=timestamp,
            first_timestamp=timestamp,
            type="Normal",
            source=V1EventSource(component="genpack"),
            involved_object=V1ObjectReference(
                kind="Pod",
                name=pod.metadata.name,
                namespace="default",
                uid=pod.metadata.uid
            )
        )
        self.api_k8s.create_namespaced_event("default", event)
        info("Event sent")

    def get_available_nodes(self, cpu: float, mem: float) -> dict:
        """
        Get the nodes with at least the given cpu and memory free
        :return: List of Pods indexed by their name
        """
        nodes = self.api_k8s.list_node(
            field_selector=("metadata.name!=hoernli-7")).items
        available_nodes = {}
        for node in nodes:
            nodename = node.metadata.name
            print(nodename)
            limit = self.metrics_storage.get_limits(nodename)
            usability = {'cpu': 0, 'memory': 0 }
            for pod in self.list_scheduled_pods_node(nodename):
                pod_info = self.get_pod_info(pod)
                usability['cpu'] += pod_info['cpu']
                usability['memory'] += pod_info['memory']
            if usability['cpu']+cpu < float(limit['cpu']) and usability['memory']+mem < float(limit['memory']):
                available_nodes[nodename] = node
        return available_nodes

    def get_nodes_scores(self, cpu: float, mem: float, nodes: dict, energy_w: float, perf_w: float) -> dict:
        """
        Compute the score of a given pod
        :param pod: name of the pod we need to compute the score
        :param nodes: the pods in the same generation as the one we compute the score
        :return: score of the pod or 0 if we have no information on the envelopes
        """
        prediction = self.model_builder.predict([[100,2]])
        score = {}
        for node in nodes.keys():
            node_prefix = node.split("-")[0]
            energy = prediction['energy'][node_prefix]
            performance = prediction['performance'][node_prefix]
            node_score = energy*energy_w + performance*perf_w
            score[node] = node_score
        return score

    def get_pod_info(self, pod: V1Pod) -> dict:
        """
        Get the resources requirements of a given pod
        :param pod: pod we need to compute the score
        :return: cpu and memory requirements
        """
        requests = pod.spec.containers[0].resources.requests
        return {
            'cpu': float(requests['cpu'].rstrip('m')), 
            'memory': float(requests['memory'].rstrip('Mi'))
        }
		
    def assign_pod_to_node(self, pod: V1Pod, node: V1Node):
        """
        Assign a pod to a node
        :param pod: type V1Pod to schedule
        :param node: type V1Node
        """
        info("Scheduling %s on %s" % (pod.metadata.name, node.metadata.name))
        binding = V1Binding(
            api_version="v1",
            kind="Binding",
            metadata=V1ObjectMeta(
                name=pod.metadata.name
            ),
            target=V1ObjectReference(
                api_version="v1",
                kind="Node",
                name=node.metadata.name
            )
        )
        try:
            self.api_k8s.create_namespaced_pod_binding(pod.metadata.name, "default", binding)
            info("Scheduled %s on %s" % (pod.metadata.name, node.metadata.name))
            self.notify_binding(pod, node)
        except Exception:
            traceback.print_exc()
            info("pod %s is already assigned" % pod.metadata.name)

    def get_duration(self, pod: V1Pod) -> int:
        """
        Compute for how long the pod has been running
        :return: Current execution time in seconds
        """  
        start = pod.status.start_time.timestamp()
        now = datetime.now().timestamp()
        return now - start

    def get_replica_pod(self, pod: V1Pod, remaining_iterations: str) -> V1Pod:
        """
        Creates a replica of the given pod
        :return: replica pod
        """ 
        pod_annotations = pod.metadata.annotations
        if pod_annotations == None:
            pod_annotations =  {'start_time': pod.status.start_time, 'creation_timestamp': pod.metadata.creation_timestamp}
        return V1Pod(
            api_version="v1",
            kind="Pod",
            metadata=V1ObjectMeta(
                name=pod.metadata.name+"-rpc",
                annotations=pod_annotations
            ),
            spec=V1PodSpec(
                scheduler_name="heater",
                containers=[V1Container(
                    name=pod.spec.containers[0].name,
                    image_pull_policy="IfNotPresent",
                    image="kmeans-static:V4",
                    command=["/usr/local/bin/run-kmeans.sh", remaining_iterations],
                    resources=V1ResourceRequirements(
                        requests={'cpu':pod.spec.containers[0].resources.requests['cpu'],'memory':pod.spec.containers[0].resources.requests['memory']},
                        limits={'cpu':pod.spec.containers[0].resources.limits['cpu'],'memory':pod.spec.containers[0].resources.limits['memory']}
                    )
                )],
                restart_policy="OnFailure"
            )
        )    

    def launch_reschedule(self):
        """
        Function used to launch a rescheduling in the cluster
        Checks if there is a better fit for the running pods and reschedules them
        """
        nodes_to_reschedule = self.list_scheduled_pods()
        for pod in nodes_to_reschedule:
            pod_info = self.get_pod_info(pod)	
            pod_cpu = pod_info['cpu']
            pod_mem = pod_info['memory']
            available_nodes = self.get_available_nodes(pod_cpu, pod_mem)
            current_host = pod.spec.node_name
            if current_host not in available_nodes.keys():
                available_nodes[current_host] = None
            pod_labels = pod.metadata.labels
            energy_weight = float(pod_labels['energy_weight'])
            performance_weight = float(pod_labels['performance_weight'])
            scores = self.get_nodes_scores(pod_cpu, pod_mem, available_nodes, energy_weight, performance_weight)	
            best_fit = max(scores.keys(), key=(lambda k: scores[k]))
            print(scores)		    
            if best_fit.split("-")[0] != current_host.split("-")[0]:
                try:
                    print("Node %s is a better fit than node %s" % (best_fit, current_host))
                    prediction = self.model_builder.predict([[100,2]])["performance"]
                    ops_per_sec = prediction[current_host.split("-")[0]]
                    if pod.spec.containers[0].command == None:
                        ops = ops_per_sec*600
                    else:
                        ops = int(pod.spec.containers[0].command[1])
                    total_time = int(ops/ops_per_sec)
                    duration = self.get_duration(pod)
                    new_duration = int(total_time - duration)
                    if new_duration > self.migration_minimum:
                        new_ops_per_sec = prediction[best_fit.split("-")[0]]
                        #remaining_iterations = str(int(new_ops_per_sec * new_duration))
                        remaining_iterations = str(int(ops - (duration*ops_per_sec)))
                        pod_rep = self.get_replica_pod(pod, remaining_iterations)
                        self.api_k8s.create_namespaced_pod("default", pod_rep)
                        self.assign_pod_to_node(pod_rep, available_nodes[best_fit])
                        self.api_k8s.delete_namespaced_pod(pod.metadata.name, pod.metadata.namespace, 
                                V1DeleteOptions(
                                    grace_period_seconds=0,
                                    orphan_dependents=True
                                )
                        )
                        print("delete %s from %s for rescheduling" % (pod.metadata.name, pod.spec.node_name))
                except Exception:
                    traceback.print_exc()
                    print("pod to delete is unreachable")
                        
def main_loop_scheduler(args):
    """
    Scheduler main program.
    :param args: arguments given by the user to customize the scheduler.
    """
    scheduler = Scheduler(args)
    last_rescheduling = datetime.now()
    while True:
        try:
            pods = scheduler.list_unscheduled_pods()
            diff = datetime.now() - last_rescheduling
            if len(pods) == 0 and diff > timedelta(seconds=scheduler.rescheduling_time):
                print("reschedule")
                scheduler.launch_reschedule()
                last_rescheduling = datetime.now()
            for pod in pods:
                pod_info = scheduler.get_pod_info(pod)
                pod_cpu = pod_info['cpu']
                pod_mem = pod_info['memory']
                available_nodes = scheduler.get_available_nodes(pod_cpu, pod_mem)
                if len(available_nodes) == 0:
                    break
                pod_labels = pod.metadata.labels
                energy_weight = float(pod_labels['energy_weight'])
                performance_weight = float(pod_labels['performance_weight'])
                scores = scheduler.get_nodes_scores(pod_cpu, pod_mem, available_nodes, energy_weight, performance_weight)
                print(scores)
                best_fit = max(scores.keys(), key=(lambda k: scores[k]))
                scheduler.assign_pod_to_node(pod, available_nodes[best_fit])
            sleep(5)
        except Exception:
            traceback.print_exc()
            info("shutting down scheduler")
            pass
