# K8s Jenkins Installation

The files in this folder help provision ci-cassandra.a.o clones into any k8s cluster.

This is used by the `.build/run-ci --only-setup` script invocation, but can also be done manually.


## One-time K8s Setup

This is a onetime setup required in a K8s cluster, required before executing `.build/run-ci --only-setup` script.  It creates the needed node-pools for different resource sized agents used in jenkins.
```
# pick a cluster name that is identifiable to you
CLUSTER_NAME="$(whoami)--cassandra-jenkins"
```
Follow the instructions according to your cloud.

### GCLOUD

```
# choose your closest (low-carbon) zone
ZONE="us-central1-c"

# cluster and controller node
gcloud container clusters create ${CLUSTER_NAME} --machine-type e2-standard-8 --disk-type=pd-ssd --num-nodes 1 --node-labels=cassandra.jenkins.controller=true --autoscaling-profile optimize-utilization --zone ${ZONE}

# small resource nodes
gcloud container node-pools create agents-small --cluster ${CLUSTER_NAME} --machine-type e2-highcpu-8 --disk-type=pd-ssd --disk-size=107 --enable-autoscaling --spot --num-nodes=0 --min-nodes=0 --max-nodes=50 --node-labels=cassandra.jenkins.agent=true,cassandra.jenkins.agent.small=true --zone ${ZONE}

# medium resource nodes
#  preference (by cost): n2-highcpu-8, c3-highcpu-8, n4-highcpu-8, n1-highcpu-16
gcloud container node-pools create agents-medium --cluster ${CLUSTER_NAME} --machine-type n2-highcpu-8 --disk-type=pd-ssd --disk-size=107 --enable-autoscaling --spot --num-nodes=0 --min-nodes=0 --max-nodes=100 --node-labels=cassandra.jenkins.agent=true,cassandra.jenkins.agent.medium=true --zone ${ZONE}

# large resource nodes
gcloud container node-pools create agents-large --cluster ${CLUSTER_NAME} --machine-type n2-standard-8 --disk-type=pd-ssd --disk-size=107 --enable-autoscaling --spot --num-nodes=0 --min-nodes=0 --max-nodes=160 --node-labels=cassandra.jenkins.agent=true,cassandra.jenkins.agent.large=true --zone ${ZONE}

# For each sized resource nodes, pick any machine type that fits, those listed above should work and be the most cost-effective, but this can change region to region
# See https://github.com/apache/cassandra/blob/cassandra-6.0/.jenkins/Jenkinsfile#L35-L38
# and agent.podTemplates.*.resourceLimitCpu and agent.podTemplates.*.resourceLimitMemory (adding gke/eks requirements) in https://github.com/apache/cassandra/blob/cassandra-6.0/.jenkins/k8s/jenkins-deployment.yaml
# The jenkins resource requirements should fit into the corresponding dind podTemplate limits.
# Remember to allow a buffer for gke/eks pods deployed on each node.
```


## Manual Jenkins Helm Installation

To manually install Jenkins into a K8s cluster using the Helm yaml (rather than using the `.build/run-ci --only-setup` invocation).

```
# auth (and make default context)
gcloud container clusters get-credentials cassius --zone ${ZONE}

helm repo add jenkins https://charts.jenkins.io
helm repo update
helm upgrade --install -f jenkins-deployment.yaml cassius jenkins/jenkins --wait

# get the server's address
kubectl describe svc cassius-jenkins | grep 'LoadBalancer Ingress'

# get the jenkins' password
kubectl exec -it svc/cassius-jenkins -c jenkins -- /bin/cat /run/secrets/additional/chart-admin-password && echo

# open http://<server_address>
```

This leaves the controller running, a single e2-standard-8 instance. All other node-pools downscale to zero.

## Upgrading an existing instance (e.g. pre-ci.cassandra.apache.org)

A long-lived site like pre-ci.cassandra.apache.org may carry customisations: hostname, cloud load-balancer, storage class; that are deliberately absent from `jenkins-deployment.yaml`.

Running  `helm upgrade -f jenkins-deployment.yaml` or `.build/run-ci --only-setup` will drop those customisations.

Instead keep the customisations in separate overrides file, using it like
```
.build/run-ci --only-setup --values-override <file>
```
 or as a second `-f` argument to `helm upgrade`.


##### To collect overridden values

To get and diff currently deployed values against the version of `jenkins-deployment.yaml` that is deployed.
```
RELEASE=cassius
NS=default
kubectl config current-context   # confirm correct context
DEPLOYED_COMMIT=cassandra-5.0 # the last jenkins-deployment.yaml deployed git commit sha 

helm get values ${RELEASE} -n ${NS} -o yaml > /tmp/cassandra-ci-live-values.yaml

git show ${DEPLOYED_COMMIT}:.jenkins/k8s/jenkins-deployment.yaml > /tmp/deployed.yaml

for f in /tmp/deployed.yaml /tmp/cassandra-ci-live-values.yaml ; do
  python3 -c 'import sys,yaml;print(yaml.safe_dump(yaml.safe_load(open(sys.argv[1])),sort_keys=True,width=10000))' ${f} > ${f}.sorted
done

diff -u /tmp/deployed.yaml.sorted /tmp/cassandra-ci-live-values.yaml.sorted
```

Copy the genuine customisations you need to keep into `~/.cassandra-ci/<site>-overrides.yaml`, keeping the full key path.

For example:
```
controller:
  ingress:
    hostName: pre-ci.cassandra.apache.org      # note the capital N, `hostname` is silently ignored
  serviceAnnotations:                          # AWS load-balancer-controller: static EIP, public subnet
    service.beta.kubernetes.io/aws-load-balancer-name: pre-ci-apache-cassandra
    ...
persistence:
  storageClass: gp2
```

Beware how Helm merges: maps are merged key by key, but lists and strings are *replaced* wholesale.  Each `agent.podTemplates.*` entry is one multi-line string, so overriding a pod template masks every repo-side change to that template.  Prefer keeping pod-template customisations out of the overrides file; where that is unavoidable, re-apply the repo's changes to the overridden copy by hand at each upgrade.


### Rolling back a bad Helm upgrade

```
helm history ${RELEASE} -n ${NS}
helm rollback ${RELEASE} <last-good-revision> -n ${NS} --wait --timeout 15m
```

`helm rollback` restores the previous chart *and* values, so the site's customisations come back with it.  If the release history itself is unusable, the `/tmp/cassandra-ci-live-values.yaml` from above can be used:
```
helm upgrade ${RELEASE} jenkins/jenkins --version ${CHART_VERSION} -n ${NS} \
    -f /tmp/cassandra-ci-live-values.yaml --wait --timeout 15m
```


### Configuring Local-only Access

If you want only local private access to Jenkins, do the following.

Comment these lines before running `helm upgrade …`
```
#  serviceType: LoadBalancer
#  ingress:
#    enabled: "true"
```
Run the helm upgrade and get the password as usual
```
helm upgrade --install -f values.yaml cassius jenkins/jenkins --wait

# get the jenkins' password
kubectl exec -it svc/cassius-jenkins -c jenkins -- /bin/cat /run/secrets/additional/chart-admin-password && echo

# port-forward 8080 to the private jenkins
kubectl port-forward svc/cassius-jenkins 8080:8080

# open http://localhost:8080
```

