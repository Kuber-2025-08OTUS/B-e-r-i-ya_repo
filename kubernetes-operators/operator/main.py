#!/usr/bin/env python3

import kubernetes.client
from kubernetes.client.rest import ApiException
from kubernetes import client, config, watch
import yaml
import time
import logging
import hashlib

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class MySQLOperator:
    def __init__(self):
        # Загрузка конфигурации Kubernetes
        try:
            config.load_incluster_config()  # Для работы внутри кластера
        except:
            config.load_kube_config()  # Для локальной разработки
        
        self.v1 = client.CoreV1Api()
        self.apps_v1 = client.AppsV1Api()
        self.custom_api = client.CustomObjectsApi()
        self.core_v1 = client.CoreV1Api()
        
        # Группа и версия нашего CRD
        self.group = "otus.homework"
        self.version = "v1"
        self.plural = "mysqls"
        
    def create_deployment(self, name, namespace, spec):
        """Создание Deployment для MySQL"""
        
        # Создание секрета для паролей
        secret_manifest = {
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": {
                "name": f"{name}-mysql-secret",
                "namespace": namespace
            },
            "type": "Opaque",
            "data": {
                "root-password": self._encode_base64(spec.get('rootPassword', 'root')),
                "password": self._encode_base64(spec.get('password', 'password'))
            }
        }
        
        try:
            self.v1.create_namespaced_secret(namespace, secret_manifest)
            logger.info(f"Secret {name}-mysql-secret created")
        except ApiException as e:
            if e.status != 409:  # 409 - уже существует
                logger.error(f"Error creating secret: {e}")
                raise
        
        # Манифест Deployment
        deployment_manifest = {
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": {
                "name": f"{name}-mysql",
                "namespace": namespace,
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
                            "image": spec.get('image', 'mysql:8.0'),
                            "env": [
                                {
                                    "name": "MYSQL_ROOT_PASSWORD",
                                    "valueFrom": {
                                        "secretKeyRef": {
                                            "name": f"{name}-mysql-secret",
                                            "key": "root-password"
                                        }
                                    }
                                },
                                {
                                    "name": "MYSQL_DATABASE",
                                    "value": spec.get('database', 'mydb')
                                },
                                {
                                    "name": "MYSQL_USER",
                                    "value": spec.get('username', 'user')
                                },
                                {
                                    "name": "MYSQL_PASSWORD",
                                    "valueFrom": {
                                        "secretKeyRef": {
                                            "name": f"{name}-mysql-secret",
                                            "key": "password"
                                        }
                                    }
                                }
                            ],
                            "ports": [{
                                "containerPort": 3306,
                                "name": "mysql"
                            }],
                            "volumeMounts": [{
                                "name": "mysql-storage",
                                "mountPath": "/var/lib/mysql"
                            }],
                            "resources": {
                                "requests": {
                                    "memory": "256Mi",
                                    "cpu": "100m"
                                },
                                "limits": {
                                    "memory": "512Mi",
                                    "cpu": "500m"
                                }
                            }
                        }],
                        "volumes": [{
                            "name": "mysql-storage",
                            "persistentVolumeClaim": {
                                "claimName": f"{name}-mysql-pvc"
                            }
                        }]
                    }
                }
            }
        }
        
        try:
            self.apps_v1.create_namespaced_deployment(namespace, deployment_manifest)
            logger.info(f"Deployment {name}-mysql created")
        except ApiException as e:
            logger.error(f"Error creating deployment: {e}")
            raise
    
    def create_service(self, name, namespace):
        """Создание Service для MySQL"""
        
        service_manifest = {
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {
                "name": f"{name}-mysql-service",
                "namespace": namespace,
                "labels": {
                    "app": "mysql",
                    "instance": name
                }
            },
            "spec": {
                "ports": [{
                    "port": 3306,
                    "targetPort": 3306,
                    "protocol": "TCP"
                }],
                "selector": {
                    "app": "mysql",
                    "instance": name
                },
                "type": "ClusterIP"
            }
        }
        
        try:
            self.v1.create_namespaced_service(namespace, service_manifest)
            logger.info(f"Service {name}-mysql-service created")
        except ApiException as e:
            logger.error(f"Error creating service: {e}")
            raise
    
    def create_pv_pvc(self, name, namespace, storage_size):
        """Создание PV и PVC для MySQL"""
        
        # Создание PersistentVolumeClaim
        pvc_manifest = {
            "apiVersion": "v1",
            "kind": "PersistentVolumeClaim",
            "metadata": {
                "name": f"{name}-mysql-pvc",
                "namespace": namespace
            },
            "spec": {
                "accessModes": ["ReadWriteOnce"],
                "resources": {
                    "requests": {
                        "storage": storage_size
                    }
                }
            }
        }
        
        try:
            self.v1.create_namespaced_persistent_volume_claim(namespace, pvc_manifest)
            logger.info(f"PVC {name}-mysql-pvc created")
        except ApiException as e:
            logger.error(f"Error creating PVC: {e}")
            raise
    
    def delete_resources(self, name, namespace):
        """Удаление всех созданных ресурсов"""
        
        resources = [
            (self.v1.delete_namespaced_secret, f"{name}-mysql-secret", namespace),
            (self.apps_v1.delete_namespaced_deployment, f"{name}-mysql", namespace),
            (self.v1.delete_namespaced_service, f"{name}-mysql-service", namespace),
            (self.v1.delete_namespaced_persistent_volume_claim, f"{name}-mysql-pvc", namespace),
        ]
        
        for delete_func, resource_name, ns in resources:
            try:
                delete_func(resource_name, ns)
                logger.info(f"Resource {resource_name} deleted")
            except ApiException as e:
                if e.status != 404:  # 404 - ресурс не найден
                    logger.error(f"Error deleting {resource_name}: {e}")
    
    def _encode_base64(self, text):
        """Кодирование строки в base64"""
        import base64
        return base64.b64encode(text.encode()).decode()
    
    def handle_mysql_cr(self, event):
        """Обработка событий MySQL Custom Resource"""
        
        obj = event['object']
        metadata = obj.get('metadata', {})
        spec = obj.get('spec', {})
        
        name = metadata.get('name')
        namespace = metadata.get('namespace', 'default')
        
        logger.info(f"Processing MySQL CR: {name} in namespace {namespace}")
        
        if event['type'] == 'ADDED' or event['type'] == 'MODIFIED':
            try:
                # Создаем ресурсы в правильном порядке
                self.create_pv_pvc(name, namespace, spec.get('storageSize', '1Gi'))
                self.create_deployment(name, namespace, spec)
                self.create_service(name, namespace)
                
                logger.info(f"Successfully created resources for MySQL CR: {name}")
                
            except Exception as e:
                logger.error(f"Error creating resources for {name}: {e}")
        
        elif event['type'] == 'DELETED':
            try:
                self.delete_resources(name, namespace)
                logger.info(f"Successfully deleted resources for MySQL CR: {name}")
            except Exception as e:
                logger.error(f"Error deleting resources for {name}: {e}")
    
    def run(self):
        """Запуск оператора"""
        logger.info("Starting MySQL Operator...")
        
        w = watch.Watch()
        
        while True:
            try:
                # Отслеживание событий для MySQL Custom Resources
                for event in w.stream(
                    self.custom_api.list_cluster_custom_object,
                    group=self.group,
                    version=self.version,
                    plural=self.plural
                ):
                    self.handle_mysql_cr(event)
                    
            except ApiException as e:
                logger.error(f"API exception: {e}")
                time.sleep(10)
            except Exception as e:
                logger.error(f"Unexpected error: {e}")
                time.sleep(10)

if __name__ == '__main__':
    operator = MySQLOperator()
    operator.run()