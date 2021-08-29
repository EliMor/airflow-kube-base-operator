import os
import yaml
import logging

from airflow.models.baseoperator import BaseOperator


class KubernetesBaseOperator(BaseOperator):
    """
    Opinionated operator for kubernetes Job type execution.
    Only allow client to pass in yaml files

    :param yaml_file_name: name of yaml file to be executed
    :param yaml_write_path:
    :param yaml_write_filename:
    :param yaml_template_fields:
    :param in_cluster: whether to use rbac inside the cluster rather than a config file
    :param config_file: a kube config file filename
    :param cluster_context: context to use referenced in the kube config file
    """

    def __init__(
        self,
        # yaml related params
        yaml_file_name,
        yaml_write_path=None,
        yaml_write_filename=None,
        yaml_template_fields={},
        # kube config related params
        in_cluster=None,
        config_file=None,
        cluster_context=None,
        # meta config
        **kwargs,
    ):
        super().__init__(**kwargs)
        logging.warning("NOT IMPLEMENTED YET")

    def execute(self, context):
        ...