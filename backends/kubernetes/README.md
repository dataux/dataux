
Kubernetes Data Source
--------------------------------------

Provides SQL Access to Kubernetes Rest API's vi the DataUX Mysql Proxy Service.

![mysql_kube](https://cloud.githubusercontent.com/assets/7269/20697265/96e13c10-b5ac-11e6-944b-c588c6e7570e.png)

**Try It Out**

**Assumes minikube is running locally**


```

# start dataux service & deployment inside your kube cluster
kubectl create -f https://raw.githubusercontent.com/dataux/dataux/master/backends/kubernetes/conf/dataux.yaml


# connect with mysql client
mysql -h $(minikube service --url --format="{{.IP}}" dataux) -P 30036

```
```sql

show databases;

use kubernetes;

show tables;

describe pods;

select name, creationtimestamp, hostip, podip, hostname from pods;

select name, creationtimestamp, hostip, podip, hostname from pods WHERE name LIKE "dataux%";

select * from nodes;

select count(*) from nodes;

select count(name), hostip from pods GROUP BY hostip;
```

**SQL examples**

```sh


mysql -h127.0.0.1 -P4000 -Dkube -e "describe pods;"
mysql -h127.0.0.1 -P4000 -Dkube -e "describe nodes;"
mysql -h127.0.0.1 -P4000 -Dkube -e "describe services;"

mysql -h127.0.0.1 -P4000 -Dkube -e "select name, creationtimestamp, hostip, podip, hostname from pods;"

mysql -h127.0.0.1 -P4000 -Dkube -e "select * from nodes;"


select count(name), hostip from pods GROUP BY hostip;


```


**Start via Download**

Install dataux https://github.com/dataux/dataux/releases

```sh

minikube start

# install dataux  https://github.com/dataux/dataux/releases
# checkout/clone github.com/dataux/dataux  to get a config file
cd github.com/dataux/dataux

# run 
./dataux --config=backends/kubernetes/kubernetes.conf


# from another terminal

mysql -h 127.0.0.1 -P 4000 -Dkube

```


Testing & Dev
-------------------------------------

We need to stand up either a local (minikube) kube cluster
or a google container engine cluster.   Then we need to decide
[how to connect to it](http://kubernetes.io/docs/user-guide/accessing-the-cluster/)

1.  via kubectl proxy (easiest)
2.  via api

* http://kubernetes.io/docs/user-guide/accessing-the-cluster/
* https://github.com/kubernetes/contrib/tree/master/ingress/controllers/nginx

**Types & Schema**
* http://kubernetes.io/docs/api-reference/v1/definitions/

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


kubectl run dataux --image=gcr.io/dataux-io/dataux:v0.0.1 --port=4000
kubectl expose deployment dataux --type=NodePort

minikube delete --v=10 --show-libmachine-logs --alsologtostderr

```


