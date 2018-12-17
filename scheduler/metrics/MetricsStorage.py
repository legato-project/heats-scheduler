from abc import ABC, abstractmethod


class MetricsStorage(ABC):
    """
    abstract base class for metrics storage:
    We can then choose to use it to request the database or give fixed data for test
    """

    @abstractmethod
    def get_generation(self, pod_name: str) -> str:
        pass

    @abstractmethod
    def get_availability(self, node_name: str) -> dict:
        pass

    @abstractmethod
    def get_envelopes(self, pod_name: str) -> dict:
        pass

    @abstractmethod
    def get_pod_metrics(self, pod_name: str) -> dict:
        pass

    @abstractmethod
    def write_point(self, measurement: str, tags: dict, fields: dict):
        pass
