```
- minikube start
- minikube addons enable ingress
- eval $(minikube docker-env)

## Применить все манифесты
kubectl apply -f mysql-crd.yaml
kubectl apply -f rbac.yaml
kubectl apply -f operator-deployment.yaml
kubectl apply -f mysql-instance.yaml

## Проверить создание CRD
kubectl get crd mysqls.otus.homework

## Проверить работу оператора
kubectl get pods -l app=mysql-operator

## Проверить создание кастомного ресурса
kubectl get mysqls.otus.homework

## Проверить созданные ресурсы
kubectl get deployment,service,pvc

## Удалить кастомный ресурс и проверить удаление зависимых ресурсов
kubectl delete mysql my-mysql-instance
kubectl get deployment,service,pvc
```

---
cd operator

# Применяем CRD
kubectl apply -f mysql-crd.yml

# Собираем и загружаем образ оператора
docker build -t berrriya/otus:mysql_operator .

# Развертываем оператор
kubectl apply -f operator-serviceaccount.yml
kubectl apply -f operator-deployment.yml

# Ждем запуска оператора
kubectl wait --for=condition=ready pod -l app=mysql-operator --timeout=60s

# Создаем экземпляр MySQL
kubectl apply -f mysql-instance.yaml

echo "MySQL operator deployed successfully!"