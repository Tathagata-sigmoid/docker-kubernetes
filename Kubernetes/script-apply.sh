kubectl create -f airflow-deployment.yml
kubectl create -f postgres-deployment.yml
kubectl create -f postgres-service.yml
kubectl create -f airflow-service.yml