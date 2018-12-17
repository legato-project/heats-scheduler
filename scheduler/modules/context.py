from logging import DEBUG, INFO

from kubernetes import config

from metrics.InfluxDB import InfluxDB

class Context:

    def __init__(self, args):
        self.scheduler_name = 'heater'
        self.rescheduling_time = args.rescheduling_time
        self.log_level = DEBUG if args.D else INFO
        self.metrics_storage = InfluxDB(args)
        config.load_kube_config()
