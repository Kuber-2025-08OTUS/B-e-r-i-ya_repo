#!/usr/bin/env python3

import kubernetes.client
from kubernetes.client.rest import ApiException
from kubernetes import client, config, watch
import yaml
import time
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class MySQLOperator:
    def __init__(self):
        # Загрузка конфигурации
        try:
            config.load_incluster_config()  # Для работы внутри кластера
        except:
            config.load_kube_config()  # Для локальной разработки

        self.v1 = client.CoreV1Api()
        self.apps_v1 = client.AppsV1Api()
        self.custom_api = client.CustomObjectsApi()
        self.core_v1 = client.CoreV1Api()

        self.group = "otus.homework"
        self.version = "v1"
        self.plural = "mysqls"
        self.namespace = "default"

    def create_deployment(self, name, spec):
        """Создание Deployment для MySQL"""
        deployment = {
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": {
                "name": f"mysql-{name}",
                "labels": {
                    "app": "mysql",
                    "instance": name
                }
            },
            "spec": {
                "replicas": 1,
                "selector": {
                    "matchLabels": {
                        "app": "mysql",
                        "instance": name
                    }
                },
                "template": {
                    "metadata": {
                        "labels": {
                            "app": "mysql",
                            "instance": name
                        }
                    },
                    "spec": {
                        "containers": [{
                            "name": "mysql",
                            "image": spec.get("image", "mysql:8.0"),
                            "env": [
                                {
                                    "name": "MYSQL_ROOT_PASSWORD",
                                    "value": spec.get("rootPassword", "root")
                                },
                                {
                                    "name": "MYSQL_DATABASE",
                                    "value": spec.get("database", "mydb")
                                },
                                {
                                    "name": "MYSQL_USER",
                                    "value": spec.get("username", "user")
                                },
                                {
                                    "name": "MYSQL_PASSWORD",
                                    "value": spec.get("password", "password")
                                }
                            ],
                            "ports": [{
                                "containerPort": 3306,
                                "name": "mysql"
                            }],
                            "volumeMounts": [{
                                "name": "mysql-storage",
                                "mountPath": "/var/lib/mysql"
                            }]
                        }],
                        "volumes": [{
                            "name": "mysql-storage",
                            "persistentVolumeClaim": {
                                "claimName": f"mysql-pvc-{name}"
                            }
                        }]
                    }
                }
            }
        }

        try:
            self.apps_v1.create_namespaced_deployment(
                namespace=self.namespace,
                body=deployment
            )
            logger.info(f"Created Deployment: mysql-{name}")
        except ApiException as e:
            logger.error(f"Exception when creating Deployment: {e}")

    def create_service(self, name):
        """Создание Service типа ClusterIP"""
        service = {
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {
                "name": f"mysql-service-{name}",
                "labels": {
                    "app": "mysql",
                    "instance": name
                }
            },
            "spec": {
                "type": "ClusterIP",
                "ports": [{
                    "port": 3306,
                    "targetPort": 3306,
                    "protocol": "TCP"
                }],
                "selector": {
                    "app": "mysql",
                    "instance": name
                }
            }
        }

        try:
            self.v1.create_namespaced_service(
                namespace=self.namespace,
                body=service
            )
            logger.info(f"Created Service: mysql-service-{name}")
        except ApiException as e:
            logger.error(f"Exception when creating Service: {e}")

    def create_pv(self, name, storage_size):
        """Создание PersistentVolume"""
        pv = {
            "apiVersion": "v1",
            "kind": "PersistentVolume",
            "metadata": {
                "name": f"mysql-pv-{name}",
                "labels": {
                    "type": "local",
                    "instance": name
                }
            },
            "spec": {
                "storageClassName": "manual",
                "capacity": {
                    "storage": storage_size
                },
                "accessModes": ["ReadWriteOnce"],
                "hostPath": {
                    "path": f"/data/mysql-{name}"
                },
                "persistentVolumeReclaimPolicy": "Retain"
            }
        }

        try:
            self.v1.create_persistent_volume(body=pv)
            logger.info(f"Created PV: mysql-pv-{name}")
        except ApiException as e:
            logger.error(f"Exception when creating PV: {e}")

    def create_pvc(self, name, storage_size):
        """Создание PersistentVolumeClaim"""
        pvc = {
            "apiVersion": "v1",
            "kind": "PersistentVolumeClaim",
            "metadata": {
                "name": f"mysql-pvc-{name}",
                "namespace": self.namespace,
                "labels": {
                    "app": "mysql",
                    "instance": name
                }
            },
            "spec": {
                "storageClassName": "manual",
                "accessModes": ["ReadWriteOnce"],
                "resources": {
                    "requests": {
                        "storage": storage_size
                    }
                },
                "volumeName": f"mysql-pv-{name}"
            }
        }

        try:
            self.v1.create_namespaced_persistent_volume_claim(
                namespace=self.namespace,
                body=pvc
            )
            logger.info(f"Created PVC: mysql-pvc-{name}")
        except ApiException as e:
            logger.error(f"Exception when creating PVC: {e}")

    def delete_resources(self, name):
        """Удаление всех созданных ресурсов"""
        resources = [
            (f"mysql-{name}", self.apps_v1.delete_namespaced_deployment),
            (f"mysql-service-{name}", self.v1.delete_namespaced_service),
            (f"mysql-pvc-{name}", self.v1.delete_namespaced_persistent_volume_claim),
            (f"mysql-pv-{name}", self.v1.delete_persistent_volume)
        ]

        for resource_name, delete_func in resources:
            try:
                if "pvc" in resource_name or "service" in resource_name:
                    delete_func(name=resource_name, namespace=self.namespace)
                elif "pv" in resource_name:
                    delete_func(name=resource_name)
                else:
                    delete_func(name=resource_name, namespace=self.namespace)
                logger.info(f"Deleted {resource_name}")
            except ApiException as e:
                if e.status != 404:  # Игнорировать если ресурс не найден
                    logger.error(f"Exception when deleting {resource_name}: {e}")

    def handle_event(self, event):
        """Обработка событий CRD"""
        obj = event["object"]
        operation = event["type"]
        name = obj["metadata"]["name"]
        spec = obj.get("spec", {})

        logger.info(f"Received event: {operation} for MySQL: {name}")

        if operation == "ADDED" or operation == "MODIFIED":
            # Создаем ресурсы
            storage_size = spec.get("storageSize", "1Gi")

            self.create_pv(name, storage_size)
            self.create_pvc(name, storage_size)
            self.create_deployment(name, spec)
            self.create_service(name)

            logger.info(f"Successfully created resources for MySQL: {name}")

        elif operation == "DELETED":
            # Удаляем ресурсы
            self.delete_resources(name)
            logger.info(f"Successfully deleted resources for MySQL: {name}")

    def run(self):
        """Запуск оператора"""
        logger.info("Starting MySQL Operator...")

        while True:
            try:
                w = watch.Watch()
                stream = w.stream(
                    self.custom_api.list_namespaced_custom_object,
                    group=self.group,
                    version=self.version,
                    plural=self.plural,
                    namespace=self.namespace
                )

                for event in stream:
                    self.handle_event(event)

            except ApiException as e:
                logger.error(f"API exception: {e}")
                time.sleep(10)
            except Exception as e:
                logger.error(f"Unexpected error: {e}")
                time.sleep(10)


if __name__ == "__main__":
    operator = MySQLOperator()
    operator.run()