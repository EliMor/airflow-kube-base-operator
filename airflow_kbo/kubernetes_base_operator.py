import os
import yaml
import logging

from airflow.models.baseoperator import BaseOperator


class KubernetesBaseOperator(BaseOperator):
    """
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
        self.yaml_file_name = yaml_file_name
        self.yaml_write_path = yaml_write_path
        self.yaml_write_filename = yaml_write_filename
        self.yaml_template_fields = yaml_template_fields

        self.in_cluster = in_cluster
        self.config_file = config_file
        self.cluster_context = cluster_context

    def _retrieve_template_from_file(self, jinja_env):
        with open(self.yaml_file_name, "r") as yaml_file_obj:
            yaml_file = yaml_file_obj.read()
            template = jinja_env.from_string(yaml_file)
            return template

    def _write_rendered_template(self, content):
        filename = self.yaml_write_filename
        if not filename:
            filename = self.yaml_file_name
        with open(self.yaml_write_path.rstrip("/") + f"/{filename}", "w") as write_file:
            write_file.write(content)

    def get_rendered_template(self, dag):
        jinja_env = dag.get_template_env()
        if dag.template_searchpath:
            template = jinja_env.get_template(self.yaml_file_name)
        else:
            template = self._retrieve_template_from_file(jinja_env)

        rendered_template = template.render(**self.yaml_template_fields)
        logging.info(f"Rendered....\n{rendered_template}")

        if self.yaml_write_path:
            self._write_rendered_template(rendered_template)

        yaml_obj = yaml.safe_load(rendered_template)
        return yaml_obj

    def execute(self, context):
        ...
