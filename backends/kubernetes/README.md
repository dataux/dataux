
Kubernetes Data Source
--------------------------------------

Provides SQL Access to Kuberntes Rest API's

* http://kubernetes.io/docs/user-guide/accessing-the-cluster/
* https://github.com/kubernetes/contrib/tree/master/ingress/controllers/nginx

**Types & Schema**
* http://kubernetes.io/docs/api-reference/v1/definitions/


![mysql_kube](https://cloud.githubusercontent.com/assets/7269/20697160/bc4a5474-b5ab-11e6-9364-69e82cc75e0d.png)


Testing & Dev
-------------------------------------

We need to stand up either a local (minikube) kube cluster
or a google container engine cluster.   Then we need to decide
[how to connect to it](http://kubernetes.io/docs/user-guide/accessing-the-cluster/)

1.  via kubectl proxy (easiest)
2.  via api
3.  ??


**Setting up Minikube**

* install minikube https://github.com/kubernetes/minikube/releases
* start/run minikube http://kubernetes.io/docs/getting-started-guides/minikube/
* start up a pod 

```sh

minikube start

minikube dashboard

# start a couple of machines
kubectl run hello-minikube --image=gcr.io/google_containers/echoserver:1.4 --port=8080
kubectl expose deployment hello-minikube --type=NodePort

kubectl get pod

curl $(minikube service hello-minikube --url)

kubectl get pods --all-namespaces

kubectl cluster-info

minikube delete --v=10 --show-libmachine-logs --alsologtostderr

```


**SQL examples**

```sh


mysql -h127.0.0.1 -P4000 -Dkube -e "describe pods;"

mysql -h127.0.0.1 -P4000 -Dkube -e "select name, creationtimestamp, hostip, podip, hostname from pods;"




```