# Cassandra Pre-commit CI Test Cluster — Setup

Estimated Time: 2 hours

## EKS Cluster Setup, via Console

1. _**Configure Cluster**_
    1. **Name:** cassandra-pre-commit-ci-test-cluster
    2. **Kubernetes version settings:** Latest (1.3.2)
    3. **Upgrade policy:** Standard
    4. **Cluster IAM role:** EKSClusterRole **(Create Recommended Role)**
    5. **Bootstrap cluster administrator access:** Allow cluster administrator access
    6. **Cluster authentication mode:** EKS API
    7. **Envelope encryption:** Default **** 
    8. **Use your own AWS KMS key:** Off
    9. **ARC Zonal shift:** Disabled **** 
2. _**Specify Networking**_
    1. **VPC:** Default
    2. **Subnets:** us-east-1a, us-east-1b
    3. **Additional security groups:** None Selected **(Configures default)**
    4. **Choose cluster IP address family:** ipv4
    5. **Configure Kubernetes service IP address block:** off
    6. **Configure remote networks to enable hybrid nodes:** off
    7. **Cluster endpoint access:** Public/Private 
3. _******Configure observability******_
    1. **Metrics - Prometheus:** Off
    2. **Metrics - CloudWatch:** On
    3. **Control plane logs**
        1. API server - On 
        2. Audit - On 
        3. Authenticator - On
        4. Controller manager - On 
        5. Scheduler - On
4. **_Select add-ons_ (Default Selection)**
    1. **Node monitoring agent**
        1. Enable automatic detection of node health issues.
    2. **kube-proxy**
        1. Enable service networking within your cluster.
    3. **Amazon VPC CNI**
        1. Enable pod networking within your cluster.
    4. **CoreDNS**
        1. Enable service discovery within your cluster.
    5. **Amazon CloudWatch Observability**
        1. Install CloudWatch Agent and enable Container Insights and Application Signals within your cluster.
    6. **Amazon EKS Pod Identity Agent**
        1. Install EKS Pod Identity Agent to use EKS Pod Identity to grant AWS IAM permissions to pods through Kubernetes service accounts.
    7. **External DNS**
        1. Install external-dns to control DNS records with Kubernetes resources.
    8. **Metrics Server**
        1. Install metrics-server to collect cluster-wide resource usage data for autoscaling and monitoring.
5. _******Configure selected add-ons settings******_
    1. **Amazon CloudWatch Observability**
        1. Version: v4.0.0-eksbuild.1
        2. Pod Identity IAM role: cloudwatch-agent (AmazonEKSPodIdentityAmazonCloudWatchObservabilityRole)
    2. **CoreDNS**
        1. Version: v1.11.4-eksbuild.2
    3. **Node monitoring agent**
        1. Version: v1.2.0-eksbuild.1
    4. **Amazon VPC CNI**
        1. Version: v1.19.2-eksbuild.1
        2. Pod Identity IAM role: vpc-cni (e.g. AmazonEKSPodIdentityVPCCNIRole)
    5. **kube-proxy**
        1. Version: v1.32.0-eksbuild.2
    6. **External DNS**
        1. Version: v0.16.1-eksbuild.2
        2. Pod Identity IAM role: external-dns (AmazonEKSPodIdentityExternalDNSRole)
    7. **Amazon EKS Pod Identity Agent**
        1. Version: v1.3.4-eksbuild.1
    8. **Metrics Server**
        1. Version: v0.7.2-eksbuild.3


Note: A few of the add-ons are initially flagged as being in “degraded” status, this will go away after compute capacity is added to cluster.

## EKS Cluster Additional Setup, via Console

### Configure EBS for Persistent Storage 

https://docs.aws.amazon.com/eks/latest/userguide/ebs-csi.html

1. **Add-on “Amazon EBS CSI Driver”**
    1. **Description:** Enable Amazon Elastic Block Storage (EBS) within your cluster.
    2. **EKS Pod Identity:  ****AmazonEKSPodIdentityAmazonEBSCSIDriverRole** (**Create Recommended Role)**
    3. **Version: **v1.43.0-eksbuild.1
2. **[Configure IAM Role](https://docs.aws.amazon.com/eks/latest/userguide/ebs-csi.html#:~:text=role%2Dname%20AmazonEKS_EBS_CSI_DriverRole-,AWS%20Management%20Console,-Open%20the%20IAM)**
    1. Create the *`AmazonEKS_EBS_CSI_DriverRole`*  IAM role and set the required trust policy

### Add Jenkins Controller Node Group 

1. **Add node group**
    1. **Node group configuration**
        1. **Name**: controller-jenkins
        2. **Node IAM role:** EKSNodeRole **(Create recommended role)**
        3. **Use Launch Template:** off
        4. **Kubernetes labels**
            1. cassandra.jenkins.controller: true
    2. ******Set compute and scaling configuration******
        1. **AMI type: **AL2023_x86_64_STANDARD
        2. **Capacity type:** On-Demand
        3. **Instance types:** m5.4xlarge
        4. **Disk size:** 150 GiB
        5. **Node group scaling configuration**
            1. Desired size: 1
            2. Minimum size: 1
            3. Maximum size: 1
        6. **Maximum unavailable:** 1 
        7. **Node auto repair configuration**
            1. **Enable node auto repai**r: On 
    3. ******Specify networking******
        1. **Subnets:** us-east-1a, us-east-1b
        2. **Configure remote access to nodes:** Off 



## Bring up the jenkins controller, via Terminal

```
# set local env w/ aws session creds 
aws eks update-kubeconfig \
  --region us-east-1 \
  --name cassandra-pre-commit-ci-test-cluster

# modify jenkins-deployment.yaml values file, 
# w/ persistent storage for aws eks (aws eks supports gp2 out of the box)
persistence:
  enabled: true
  size: "500Gi"
  storageClass: "gp2"

# install the heml chart 
helm upgrade --install \
  -f jenkins-deployment.yaml \
  cassius jenkins/jenkins \
  --wait \
  --debug 
  
# if need to debug 
kubectl get events --sort-by='.lastTimestamp' -A

# verify jenkins is running
kubectl get pods
NAME                READY   STATUS    RESTARTS   AGE
cassius-jenkins-0   2/2     Running   0          3m44s


curl checkip.amazonaws.com
98.247.34.70

# modify jenkins-deployment.yaml values file to set load balancer
# allowed ingress 
loadBalancerSourceRanges:
  - "98.247.34.70/32"
  
# apply the ingress rule 
helm upgrade --install \
  -f jenkins-deployment.yaml \
  cassius jenkins/jenkins \
  --wait \
  --debug 
  
# get the load balancer ip 
export SERVICE_IP=$(kubectl get svc --namespace default cassius-jenkins --template "{{ range (index .status.loadBalancer.ingress 0) }}{{ . }}{{ end }}")
host $SERVICE_IP
acc5b412c2cd94c10a23d011cb02b5b3-467084034.us-east-1.elb.amazonaws.com has address 34.194.107.220
acc5b412c2cd94c10a23d011cb02b5b3-467084034.us-east-1.elb.amazonaws.com has address 52.1.174.232

# sanity check connectivity to the URL (logoff aws corp vpn if on it, as there's a firewall on 8080)
curl 34.194.107.220:8080 

# visit jenkins URL in the browser
# on top right of the page, click login
# set username as 'admin'
# set password as: 
JENKINS_PASSWORD=$(kubectl exec --namespace default -it svc/cassius-jenkins -c jenkins -- /bin/cat /run/secrets/additional/chart-admin-password && echo)

# navigate to Cassandra > Build with Parameters
# Set build parameters, as like:
# repository: https://github.com/apache/cassandra
# branch: trunk
# profile: skinny
# architecture: amd64
# jdk: 11
# (the set as keep empty)

# Submit the Build 

# in the console output, note that it'll hang at:
`‘`[agent-dind-small-84040](http://34.194.107.220:8080/computer/agent%2Ddind%2Dsmall%2D84040/)`’ is offline

# next, configure the jenkins agent node pools and cluster auto scaler...`
```

Note: Only two modifications to the jenkins-deployment.yaml (from the tlp repo) were needed: 1/ gp2 storage 2/ LB ingress to prevent wide open access while iterating on prototype)

## Configure Jenkins agent node pools and cluster auto scaler

### Add {small, medium, large} node groups 

The Jenkinsfile requests the following (jenkins) agent resource capacities 

```
//  - cassandra-amd64-small  : 1 cpu, 1GB ram
//  - cassandra-amd64-medium : 3 cpu, 5GB ram
//  - cassandra-amd64-large  : 7 cpu, 14GB 
```

So, we’ll set:

* Small:  ~~2 vCPU, 4 GiB~~ (ran into errors with cpu requirement matching at pod scheduling)
    [~~https://instances.vantage.sh/aws/ec2/c7a.large~~](https://instances.vantage.sh/aws/ec2/c7a.large)
    4vCPU, 8 GiB
     https://instances.vantage.sh/aws/ec2/c7a.xlarge
* Medium: 4vCPU, 8 GiB
     https://instances.vantage.sh/aws/ec2/c7a.xlarge
* Large: 8 vCPU, 32 GiB 
    https://instances.vantage.sh/aws/ec2/m7a.2xlarge?currency=USD

#### Small node group 

1. **Add node group**
    1. **Node group configuration**
        1. **Name**: amd64-small
        2. **Node IAM role: **EKSNodeRole** (Create recommended role)**
        3. **Use Launch Template:** off
        4. **Kubernetes labels**
            1. cassandra.jenkins.agent.small: true
            2. cassandra.jenkins.agent: true
    2. ******Set compute and scaling configuration******
        1. **AMI type: **AL2023_x86_64_STANDARD
        2. **Capacity type: **On-Demand
        3. **Instance types:** [c7a.xlarge](https://instances.vantage.sh/aws/ec2/c7a.xlarge)
        4. **Disk size:** 50 GiB
        5. **Node group scaling configuration**
            1. Desired size: 0
            2. Minimum size: 0
            3. Maximum size: 100
        6. **Maximum unavailable: **1 
        7. **Node auto repair configuration**
            1. **Enable node auto repai**r: Off 
    3. ******Specify networking******
        1. **Subnets:** us-east-1a, us-east-1b
        2. **Configure remote access to nodes: **Off 

#### Medium Node Group

1. **Add node group**
    1. **Node group configuration**
        1. **Name**: amd64-medium
        2. **Node IAM role: **EKSNodeRole** (Create recommended role)**
        3. **Use Launch Template:** off
        4. **Kubernetes labels**
            1. cassandra.jenkins.agent.medium: true
            2. cassandra.jenkins.agent: true
    2. ******Set compute and scaling configuration******
        1. **AMI type: **AL2023_x86_64_STANDARD
        2. **Capacity type: **On-Demand
        3. **Instance types:** [c7a.xlarge](https://instances.vantage.sh/aws/ec2/c7a.xlarge)
        4. **Disk size: **50 GiB
        5. **Node group scaling configuration**
            1. Desired size: 0
            2. Minimum size: 0
            3. Maximum size: 100
        6. **Maximum unavailable: **1 
        7. **Node auto repair configuration**
            1. **Enable node auto repai**r: Off 
    3. ******Specify networking******
        1. **Subnets:** us-east-1a, us-east-1b
        2. **Configure remote access to nodes: **Off 

#### Large Node Group

1. **Add node group**
    1. **Node group configuration**
        1. **Name**: amd64-large
        2. **Node IAM role: **EKSNodeRole** (Create recommended role)**
        3. **Use Launch Template:** off
        4. **Kubernetes labels**
            1. cassandra.jenkins.agent.large: true
            2. cassandra.jenkins.agent: true
    2. ******Set compute and scaling configuration******
        1. **AMI type: **AL2023_x86_64_STANDARD
        2. **Capacity type: **On-Demand
        3. **Instance types:** [c7a.2xlarge](https://instances.vantage.sh/aws/ec2/c7a.2xlarge)
        4. **Disk size: **50 GiB
        5. **Node group scaling configuration**
            1. Desired size: 0
            2. Minimum size: 0
            3. Maximum size: 100
        6. **Maximum unavailable: **1 
        7. **Node auto repair configuration**
            1. **Enable node auto repai**r: Off 
    3. ******Specify networking******
        1. **Subnets:** us-east-1a, us-east-1b
        2. **Configure remote access to nodes: **Off 



### Configure [Cluster Autoscaler](https://docs.aws.amazon.com/eks/latest/best-practices/cas.html)

https://github.com/kubernetes/autoscaler/blob/master/cluster-autoscaler/cloudprovider/aws/README.md

https://docs.aws.amazon.com/eks/latest/userguide/associate-service-account-role.html

#### 1. Create the Cluster Autoscaler IAM Permission Policy

https://github.com/kubernetes/autoscaler/blob/master/cluster-autoscaler/cloudprovider/aws/README.md#full-cluster-autoscaler-features-policy-recommended

* IAM > Policies > Create Policy

```
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "autoscaling:SetDesiredCapacity",
                "autoscaling:TerminateInstanceInAutoScalingGroup"
            ],
            "Resource": "*",
            "Condition": {
                "StringEquals": {
                    "aws:ResourceTag/k8s.io/cluster-autoscaler/enabled": "true",
                    "aws:ResourceTag/k8s.io/cluster-autoscaler/cassandra-pre-commit-ci-test-cluster": "owned"
                }
            }
        },
        {
            "Effect": "Allow",
            "Action": [
                "autoscaling:DescribeAutoScalingGroups",
                "autoscaling:DescribeAutoScalingInstances",
                "autoscaling:DescribeLaunchConfigurations",
                "autoscaling:DescribeScalingActivities",
                "autoscaling:DescribeTags",
                "ec2:DescribeImages",
                "ec2:DescribeInstanceTypes",
                "ec2:DescribeLaunchTemplateVersions",
                "ec2:GetInstanceTypesFromInstanceRequirements",
                "eks:DescribeNodegroup"
            ],
            "Resource": "*"
        }
    ]
}
```

* **Policy Name**: EksClusterAutoScalerPolicy

#### 2.  Create the IAM Role Kubernetes Service Account for the Cluster Autoscaler

Pre-requisite:  Before creating the EKS IAM service account for the cluster autoscaler, there's a required step to configure the IAM OIDC provider for the cluster, i.e. https://docs.aws.amazon.com/eks/latest/userguide/enable-iam-roles-for-service-accounts.html#_create_oidc_provider_console

```
`eksctl create iamserviceaccount \`
`    ``--``name cluster``-``autoscaler``-``service``-``account`` \`
`    ``--``namespace`` kube``-``system`` \`
`    ``--``cluster cassandra``-``pre``-``commit``-``ci``-``test``-``cluster`` \`
`    ``--``role``-``name ``EKSClusterAutoscalerServiceAccountRole`` \`
`    ``--``attach``-``policy``-``arn arn:aws:iam::564507210442:policy/EksClusterAutoScalerPolicy \
    --region us-east-1 \`
`    ``--``approve`
```

#### 3. Install the Cluster Autoscaler Helm Chart

https://eksworkshop.com/docs/autoscaling/compute/cluster-autoscaler/install

```
helm repo add autoscaler https://kubernetes.github.io/autoscaler

export CLUSTER_AUTOSCALER_CHART_VERSION=9.38.0
export EKS_CLUSTER_NAME=cassandra-pre-commit-ci-test-cluster
export AWS_REGION=us-east-1
export CLUSTER_AUTOSCALER_IMAGE_TAG=1.31.0
export CLUSTER_AUTOSCALER_ROLE='arn:aws:iam::564507210442:role/EKSClusterAutoscalerServiceAccountRole'

helm upgrade --install cluster-autoscaler autoscaler/cluster-autoscaler \
  --version "${CLUSTER_AUTOSCALER_CHART_VERSION}" \
  --namespace "kube-system" \
  --set "autoDiscovery.clusterName=${EKS_CLUSTER_NAME}" \
  --set "awsRegion=${AWS_REGION}" \
  --set "image.tag=v${CLUSTER_AUTOSCALER_IMAGE_TAG}" \
  --set "rbac.serviceAccount.name=cluster-autoscaler-service-account" \
  `--`*`set`*` ``"rbac.serviceAccount.create=false"`` \ `
  --set "rbac.serviceAccount.annotations.eks\\.amazonaws\\.com/role-arn"="$CLUSTER_AUTOSCALER_ROLE" \
  --wait

kubectl --namespace=kube-system get pods -l "app.kubernetes.io/name=aws-cluster-autoscaler,app.kubernetes.io/instance=cluster-autoscaler"
NAME                                                         READY   STATUS    RESTARTS   AGE
cluster-autoscaler-aws-cluster-autoscaler-6f7d5bfc6f-nsvzc   1/1     Running   0          29s
```

#### 4. Validate pod autoscaling

#### Scaling up from 0, from the initial job submitted at [Bring up the jenkins controller, via Terminal](https://quip-amazon.com/pBImAXi0IzIA#temp:C:dBN2c97a18289094c0cba1a1ad79)

```
kubectl get pods
NAME                     READY   STATUS    RESTARTS   AGE
agent-dind-small-hgtkc   2/2     Running   0          2m46s
agent-dind-small-zsqz0   2/2     Running   0          33s
cassius-jenkins-0        2/2     Running   0          132m
```

```
kubectl logs -n kube-system cluster-autoscaler-aws-cluster-autoscaler-6f7d5bfc6f-nsvzc
...
`I0521 ``04``:``34``:``30.111719`` ``1`` orchestrator``.``go``:``255``]`` ``Final`` scale``-``up plan``:`` ``[{``eks``-``amd64``-``small``-``6acb788f``-``0d20``-``2806``-``9328``-``081871e329d2`` ``0``->``1`` ``(``max``:`` ``100``)}]`
`I0521 ``04``:``34``:``30.111736`` ``1`` executor``.``go``:``166``]`` ``Scale``-``up``:`` setting ``group`` eks``-``amd64``-``small``-``6acb788f``-``0d20``-``2806``-``9328``-``081871e329d2`` size to ``1`
`...`
`I0521 ``04``:``35``:``40.520953`` ``1`` orchestrator``.``go``:``255``]`` ``Final`` scale``-``up plan``:`` ``[{``eks``-``amd64``-``small``-``6acb788f``-``0d20``-``2806``-``9328``-``081871e329d2`` ``1``->``2`` ``(``max``:`` ``100``)}]`
`I0521 ``04``:``35``:``40.520970`` ``1`` executor``.``go``:``166``]`` ``Scale``-``up``:`` setting ``group`` eks``-``amd64``-``small``-``6acb788f``-``0d20``-``2806``-``9328``-``081871e329d2`` size to ``2` 
...
```

#### Submitting a pre-commit job

```
# navigate to Cassandra > Build with Parameters
# Set build parameters, as like:
# repository: https://github.com/apache/cassandra
# branch: trunk
# profile:pre-commit
# architecture: amd64
# jdk: 11
```

```
date -u
Wed May 21 04:47:49 UTC 2025

cassandra-ci-sandbox-infra-tests % kubectl get pods
NAME                      READY   STATUS    RESTARTS   AGE
agent-dind-large-00k0g    0/2     Pending   0          4s
agent-dind-large-00v61    0/2     Pending   0          3s
agent-dind-large-0km8d    0/2     Pending   0          3s
agent-dind-large-0n45l    0/2     Pending   0          3s
agent-dind-large-0xwdp    0/2     Pending   0          3s
agent-dind-large-13t8z    0/2     Pending   0          3s
agent-dind-large-1rxcz    0/2     Pending   0          3s
agent-dind-large-1wn8s    0/2     Pending   0          4s
agent-dind-large-40tnq    0/2     Pending   0          3s
agent-dind-large-4fwd0    0/2     Pending   0          4s
agent-dind-large-58237    0/2     Pending   0          4s
agent-dind-large-5b6d2    0/2     Pending   0          3s
agent-dind-large-5bx4t    0/2     Pending   0          3s
agent-dind-large-5dxfn    0/2     Pending   0          3s
agent-dind-large-5zv5w    0/2     Pending   0          3s
agent-dind-large-6vv93    0/2     Pending   0          4s
agent-dind-large-7dwb7    0/2     Pending   0          4s
agent-dind-large-7n3t3    0/2     Pending   0          3s
agent-dind-large-81d4k    0/2     Pending   0          3s
agent-dind-large-8wkl6    0/2     Pending   0          4s
agent-dind-large-90f0d    0/2     Pending   0          3s
agent-dind-large-91pbl    0/2     Pending   0          4s
agent-dind-large-925s1    0/2     Pending   0          3s
agent-dind-large-9cnvn    0/2     Pending   0          3s
agent-dind-large-9ct3j    0/2     Pending   0          3s
agent-dind-large-9hkdr    0/2     Pending   0          3s
agent-dind-large-b50kt    0/2     Pending   0          3s
agent-dind-large-b8tz0    0/2     Pending   0          3s
agent-dind-large-bxl2f    0/2     Pending   0          4s
agent-dind-large-ccsqp    0/2     Pending   0          4s
agent-dind-large-cfnhs    0/2     Pending   0          4s
agent-dind-large-dcbjt    0/2     Pending   0          3s
agent-dind-large-dwlt3    0/2     Pending   0          3s
agent-dind-large-fpnvp    0/2     Pending   0          3s
agent-dind-large-ftw6f    0/2     Pending   0          3s
agent-dind-large-gxmtl    0/2     Pending   0          3s
agent-dind-large-j9r10    0/2     Pending   0          3s
agent-dind-large-jc83x    0/2     Pending   0          3s
agent-dind-large-jhd48    0/2     Pending   0          3s
agent-dind-large-l073j    0/2     Pending   0          4s
agent-dind-large-l6g7w    0/2     Pending   0          3s
agent-dind-large-lmh2t    0/2     Pending   0          3s
agent-dind-large-lsbxj    0/2     Pending   0          3s
agent-dind-large-lw6zg    0/2     Pending   0          4s
agent-dind-large-m165z    0/2     Pending   0          4s
agent-dind-large-mcljs    0/2     Pending   0          3s
agent-dind-large-mlx4d    0/2     Pending   0          3s
agent-dind-large-nnf5h    0/2     Pending   0          4s
agent-dind-large-ph99w    0/2     Pending   0          3s
agent-dind-large-qf80b    0/2     Pending   0          3s
agent-dind-large-qs86h    0/2     Pending   0          4s
agent-dind-large-rtktg    0/2     Pending   0          4s
agent-dind-large-s0tt2    0/2     Pending   0          3s
agent-dind-large-s9s6r    0/2     Pending   0          4s
agent-dind-large-t3jdp    0/2     Pending   0          3s
agent-dind-large-tsjhq    0/2     Pending   0          3s
agent-dind-large-v5rkc    0/2     Pending   0          3s
agent-dind-large-vm8tx    0/2     Pending   0          4s
agent-dind-large-w1135    0/2     Pending   0          3s
agent-dind-large-z1npp    0/2     Pending   0          3s
agent-dind-large-z2530    0/2     Pending   0          3s
agent-dind-large-zn1tv    0/2     Pending   0          3s
agent-dind-large-zs2wb    0/2     Pending   0          4s
agent-dind-large-zv905    0/2     Pending   0          3s
agent-dind-medium-2chzc   0/2     Pending   0          3s
agent-dind-medium-3tq0t   0/2     Pending   0          3s
agent-dind-medium-45t1j   0/2     Pending   0          3s
agent-dind-medium-5vnm8   0/2     Pending   0          3s
agent-dind-medium-6hx3p   0/2     Pending   0          3s
agent-dind-medium-6nckm   0/2     Pending   0          3s
agent-dind-medium-7kp94   0/2     Pending   0          3s
agent-dind-medium-8ddlb   0/2     Pending   0          3s
agent-dind-medium-8msz0   0/2     Pending   0          4s
agent-dind-medium-9f8n6   0/2     Pending   0          3s
agent-dind-medium-9k7q6   0/2     Pending   0          3s
agent-dind-medium-9zn2w   0/2     Pending   0          3s
agent-dind-medium-b8w1j   0/2     Pending   0          3s
agent-dind-medium-fq2mr   0/2     Pending   0          3s
agent-dind-medium-g6jdk   0/2     Pending   0          3s
agent-dind-medium-hm25d   0/2     Pending   0          3s
agent-dind-medium-kqpdd   0/2     Pending   0          3s
agent-dind-medium-l0rtc   0/2     Pending   0          3s
agent-dind-medium-ldmg2   0/2     Pending   0          3s
agent-dind-medium-lkftf   0/2     Pending   0          3s
agent-dind-medium-mk91s   0/2     Pending   0          3s
agent-dind-medium-mqlf4   0/2     Pending   0          3s
agent-dind-medium-mqln9   0/2     Pending   0          3s
agent-dind-medium-mwzfn   0/2     Pending   0          3s
agent-dind-medium-nclmh   0/2     Pending   0          3s
agent-dind-medium-p7rfl   0/2     Pending   0          3s
agent-dind-medium-tr3jr   0/2     Pending   0          3s
agent-dind-medium-xblrx   0/2     Pending   0          3s
agent-dind-medium-z2z6k   0/2     Pending   0          3s
agent-dind-medium-znxsn   0/2     Pending   0          3s
agent-dind-medium-zsfz4   0/2     Pending   0          3s
agent-dind-small-9779f    0/2     Pending   0          4s
agent-dind-small-hgtkc    2/2     Running   0          9m43s
agent-dind-small-zsqz0    2/2     Running   0          7m30s
cassius-jenkins-0         2/2     Running   0          139m
```

#### 5. Validate auto scale back down

```
# Start, Wed May 21 04:47:49 UTC 2025
# 40 minutes later
date -u
Wed May 21 05:35:49 UTC 2025

kubectl get pods
NAME                      READY   STATUS        RESTARTS   AGE
agent-dind-medium-1vdh7   2/2     Running       0          38m
agent-dind-medium-3h2ls   2/2     Running       0          38m
agent-dind-medium-437v6   2/2     Terminating   0          38m
agent-dind-medium-7fkrn   2/2     Running       0          38m
agent-dind-medium-7s6w7   2/2     Running       0          38m
agent-dind-medium-81srt   2/2     Running       0          38m
agent-dind-medium-bjp0x   2/2     Running       0          38m
agent-dind-medium-dn18r   2/2     Running       0          15m
agent-dind-medium-mzgpc   2/2     Running       0          38m
agent-dind-medium-p7t40   2/2     Running       0          38m
agent-dind-medium-q4n05   2/2     Running       0          8m2s
agent-dind-medium-ql724   2/2     Running       0          38m
agent-dind-small-hgtkc    2/2     Running       0          59m
agent-dind-small-zsqz0    2/2     Running       0          57m
cassius-jenkins-0         2/2     Running       0          3h10m

```


* * *

## Other

* **EKS Cluster Version Upgrades:**
    
    EKS Cluster Versions have a period of “support, until they are auto-upgraded. For this setup, it should be ~+16 months. Given that we are only using standard/default ‘add-ons’, it should be mostly expected that the auto-upgrade is smooth. 
    
    
* **OS Patching**
    
    If Amazon Machine Images are used, EKS provides the upgraded image versions. Applying them is a one-click. operation is console. The `eksctl` cli also has `eksctl upgrade nodegroup`.
    

* * *

# Appendix

### jenkins-deployment.yaml

AWS EKS supports gp2 storage class as the default choice; Additional configuration would be needed to use gp3. 

An elastic IP is registered with the load balancer to preserve a static IP on subsequent `helm destroy, helm install/upgrade`

```
# https://github.com/jenkinsci/helm-charts/tree/main/charts/jenkins
persistence:
  enabled: true
  size: "500Gi"
  storageClass: "gp2"
controller:
  # To get URL run `kubectl describe svc cassius-jenkins | grep 'LoadBalancer Ingress'` and use port 8080
  serviceType: LoadBalancer
  serviceAnnotations:
    service.beta.kubernetes.io/aws-load-balancer-name: pre-ci-apache-cassandra
    service.beta.kubernetes.io/aws-load-balancer-scheme: internet-facing
    service.beta.kubernetes.io/aws-load-balancer-subnets: subnet-0fcaca1372fd0bdf9
    service.beta.kubernetes.io/aws-load-balancer-eip-allocations: eipalloc-08486b2684b914189
  serviceLabels: 
    service.beta.kubernetes.io/aws-load-balancer-name: pre-ci-apache-cassandra
    service.beta.kubernetes.io/aws-load-balancer-scheme: internet-facing
    service.beta.kubernetes.io/aws-load-balancer-subnets: subnet-0fcaca1372fd0bdf9
    service.beta.kubernetes.io/aws-load-balancer-eip-allocations: eipalloc-08486b2684b914189
  ingress:
   enabled: true
   hostname: pre-ci.cassandra.apache.org
  customJenkinsLabels:
    - controller
  resources:
    requests:
      cpu: 4
      memory: 16G
    limits:
      cpu: 8
      memory: 20G
  # jenkinsOpts: "--httpPort=80"  # Tell Jenkins to listen on port 80
  javaOpts: -server -XX:+AlwaysPreTouch -XX:+UseG1GC -XX:+ExplicitGCInvokesConcurrent -XX:+ParallelRefProcEnabled -XX:+UseStringDeduplication -XX:+UnlockExperimentalVMOptions -XX:G1NewSizePercent=40 -Xms8G -Xmx8G
  installPlugins:
    - job-dsl
    - configuration-as-code
    - kubernetes
    - git
    - workflow-job
    - workflow-cps
    - junit
    - workflow-aggregator
    - pipeline-graph-view
    - ws-cleanup
    - pipeline-build-step
    - pipeline-rest-api
    - test-stability
    - copyartifact
  node-selector:
    cassandra.jenkins.controller: true
  scriptApproval:
    - "staticMethod java.lang.System setProperty java.lang.String java.lang.String"
    - "staticMethod org.codehaus.groovy.runtime.DefaultGroovyMethods combinations java.util.Collection"
    - "staticMethod org.codehaus.groovy.runtime.DefaultGroovyMethods inspect java.lang.Object"
    - "staticMethod org.codehaus.groovy.runtime.DefaultGroovyMethods max java.util.Collection"
    - "staticMethod org.codehaus.groovy.runtime.DefaultGroovyMethods putAt java.util.List java.util.List java.lang.Object"  
  JCasC:
    configScripts:
      welcome-message: |
        jenkins:
          systemMessage: Welcome to Apache Cassandra
      test-job: |
        jobs:
          - script: >
              pipelineJob('cassandra') {
                definition {
                  cpsScm {
                    scm {
                      git {
                        remote {
                          url('https://github.com/apache/cassandra')
                        }
                        branch('cassandra-5.0')
                        scriptPath('.jenkins/Jenkinsfile')
                      }
                    }
                    lightweight()
                  }
                }
              }
    globalDefaultFlowDurabilityLevel:
      durabilityHint: "PERFORMANCE_OPTIMIZED"
    securityRealm: |-
      local:
        allowsSignup: false
        enableCaptcha: false
        users:
        - id: "admin"
          name: "Jenkins Admin"
          password: "${chart-admin-password}"
    authorizationStrategy: |-
      loggedInUsersCanDoAnything:
        allowAnonymousRead: true
    googlePodMonitor:
        enabled: true
agent:
  disableDefaultAgent: true
  maxRequestsPerHostStr: "3200"
  containerCap: 300
  node-selector:
    cassandra.jenkins.agent: true
  waitForPodSec: "180"
  podTemplates:
    agent-dind-small: |
      - name: agent-dind-small
        label: agent-dind cassandra-small cassandra-amd64-small
        nodeSelector: 'cassandra.jenkins.agent.small=true'
        activeDeadlineSeconds: '0'
        idleMinutes: 1
        instanceCap: 50
        instanceCapStr: "50"
        nodeUsageMode: "NORMAL"
        showRawYaml: 'true'
        slaveConnectTimeout: '30'
        yamlMergeStrategy: override
        containers:
          - name: jnlp
            # https://github.com/jenkinsci/kubernetes-plugin#pipeline-support
            alwaysPullImage: true
            envVars:
              - envVar:
                  key: DOCKER_TLS_CERTDIR
                  value: /certs/client/
              - envVar:
                  key: DOCKER_CERT_PATH
                  value: /certs/client/
              - envVar:
                  key: DOCKER_TLS_VERIFY
                  value: 'true'
              - envVar:
                  key: DOCKER_HOST
                  value: tcp://localhost:2376
              - envVar:
                  key: JENKINS_JAVA_OPTS
                  value: '-Dorg.jenkinsci.plugins.durabletask.BourneShellScript.USE_BINARY_WRAPPER=true -Xlog:gc+heap+exit -XX:+HeapDumpOnOutOfMemoryError'
            # there's a lot of docker pulls,
            # TODO implement option for docker registry caches :: https://medium.com/@elementtech.dev/kubernetes-image-proxy-cache-from-minutes-to-milliseconds-fd14173e831f
            image: apache.jfrog.io/cassan-docker/apache/cassandra-jenkins-k8s
            livenessProbe:
              failureThreshold: '0'
              initialDelaySeconds: '0'
              periodSeconds: '0'
              successThreshold: '0'
              timeoutSeconds: '0'
            privileged: 'true'
            resourceRequestCpu: 1
            resourceLimitCpu: 2
            resourceRequestMemory: 1G
            resourceLimitMemory: 1G
            ttyEnabled: 'true'
            workingDir: /home/jenkins/agent
          - name: dind
            alwaysPullImage: 'false'
            envVars:
            - envVar:
                key: DOCKER_TLS_CERTDIR
                value: /certs
            - envVar:
                key: "DOCKER_IPTABLES_LEGACY"
                value: "1"
            image: docker:dind
            args: "--default-address-pool base=192.168.96.0/20,size=24"  # overwrite docker subnet in case of overlapping 
            livenessProbe:
              failureThreshold: '0'
              initialDelaySeconds: '0'
              periodSeconds: '0'
              successThreshold: '0'
              timeoutSeconds: '0'
            privileged: 'true'
            resourceRequestCpu: 2
            resourceLimitCpu: 4
            resourceRequestMemory: 1G
            resourceLimitMemory: 2400M
            ttyEnabled: 'true'
            workingDir: /home/jenkins/agent
        volumes:
          - emptyDirVolume:
              memory: 'false'
              mountPath: /var/lib/docker
          - emptyDirVolume:
              memory: 'false'
              mountPath: /certs
    agent-dind-medium: |
      - name: agent-dind-medium
        label: agent-dind cassandra-medium cassandra-amd64-medium
        nodeSelector: 'cassandra.jenkins.agent.medium=true'
        activeDeadlineSeconds: '0'
        idleMinutes: 1
        instanceCap: 100
        instanceCapStr: "100"
        nodeUsageMode: "NORMAL"
        showRawYaml: 'true'
        slaveConnectTimeout: '30'
        yamlMergeStrategy: override
        containers:
          - name: jnlp
            # https://github.com/jenkinsci/kubernetes-plugin#pipeline-support
            alwaysPullImage: true
            envVars:
              - envVar:
                  key: DOCKER_TLS_CERTDIR
                  value: /certs/client/
              - envVar:
                  key: DOCKER_CERT_PATH
                  value: /certs/client/
              - envVar:
                  key: DOCKER_TLS_VERIFY
                  value: 'true'
              - envVar:
                  key: DOCKER_HOST
                  value: tcp://localhost:2376
              - envVar:
                  key: JENKINS_JAVA_OPTS
                  value: '-Dorg.jenkinsci.plugins.durabletask.BourneShellScript.USE_BINARY_WRAPPER=true'
            image: apache.jfrog.io/cassan-docker/apache/cassandra-jenkins-k8s
            livenessProbe:
              failureThreshold: '0'
              initialDelaySeconds: '0'
              periodSeconds: '0'
              successThreshold: '0'
              timeoutSeconds: '0'
            privileged: 'true'
            resourceRequestCpu: 1
            resourceLimitCpu: 3
            resourceRequestMemory: 1G
            resourceLimitMemory: 2400M
            ttyEnabled: 'true'
            workingDir: /home/jenkins/agent
          - name: dind
            alwaysPullImage: 'false'
            envVars:
            - envVar:
                key: DOCKER_TLS_CERTDIR
                value: /certs
            - envVar:
                key: "DOCKER_IPTABLES_LEGACY"
                value: "1"
            image: docker:dind
            args: "--default-address-pool base=192.168.96.0/20,size=24"  # overwrite docker subnet in case of overlapping
            livenessProbe:
              failureThreshold: '0'
              initialDelaySeconds: '0'
              periodSeconds: '0'
              successThreshold: '0'
              timeoutSeconds: '0'
            privileged: 'true'
            resourceRequestCpu: 2
            resourceLimitCpu: 4
            resourceRequestMemory: 3400M
            resourceLimitMemory: 5G
            ttyEnabled: 'true'
            workingDir: /home/jenkins/agent
        volumes:
          - emptyDirVolume:
              memory: 'false'
              mountPath: /var/lib/docker
          - emptyDirVolume:
              memory: 'false'
              mountPath: /certs
    agent-dind-large: |
      - name: agent-dind-large
        label: agent-dind cassandra-amd64-large
        nodeSelector: 'cassandra.jenkins.agent.large=true'
        activeDeadlineSeconds: '0'
        idleMinutes: 1
        instanceCap: 200
        instanceCapStr: "200"
        nodeUsageMode: "NORMAL"
        showRawYaml: 'true'
        slaveConnectTimeout: '30'
        yamlMergeStrategy: override
        containers:
          - name: jnlp
            # https://github.com/jenkinsci/kubernetes-plugin#pipeline-support
            alwaysPullImage: true
            envVars:
              - envVar:
                  key: DOCKER_TLS_CERTDIR
                  value: /certs/client/
              - envVar:
                  key: DOCKER_CERT_PATH
                  value: /certs/client/
              - envVar:
                  key: DOCKER_TLS_VERIFY
                  value: 'true'
              - envVar:
                  key: DOCKER_HOST
                  value: tcp://localhost:2376
              - envVar:
                  key: JENKINS_JAVA_OPTS
                  value: '-Dorg.jenkinsci.plugins.durabletask.BourneShellScript.USE_BINARY_WRAPPER=true'
            image: apache.jfrog.io/cassan-docker/apache/cassandra-jenkins-k8s
            livenessProbe:
              failureThreshold: '0'
              initialDelaySeconds: '0'
              periodSeconds: '0'
              successThreshold: '0'
              timeoutSeconds: '0'
            privileged: 'true'
            resourceRequestCpu: 1
            resourceLimitCpu: 3
            resourceRequestMemory: 1G
            resourceLimitMemory: 2G
            ttyEnabled: 'true'
            workingDir: /home/jenkins/agent
          - name: dind
            alwaysPullImage: 'false'
            envVars:
            - envVar:
                key: DOCKER_TLS_CERTDIR
                value: /certs
            - envVar:
                key: "DOCKER_IPTABLES_LEGACY"
                value: "1"
            image: docker:dind
            args: "--default-address-pool base=192.168.96.0/20,size=24"  # overwrite docker subnet in case of overlapping
            livenessProbe:
              failureThreshold: '0'
              initialDelaySeconds: '0'
              periodSeconds: '0'
              successThreshold: '0'
              timeoutSeconds: '0'
            privileged: 'true'
            resourceRequestCpu: 6
            resourceLimitCpu: 7
            resourceRequestMemory: 13G
            resourceLimitMemory: 16G
            ttyEnabled: 'true'
            workingDir: /home/jenkins/agent
        volumes:
          - emptyDirVolume:
              memory: 'false'
              mountPath: /var/lib/docker
          - emptyDirVolume:
              memory: 'false'
              mountPath: /certs



```






## EKS Cluster Configuration Summary — Console Creation

### Cluster Details

* **Name**: cassandra-pre-commit-ci-test-cluster
* **Kubernetes Version**: 1.32
* **EKS Auto Mode**: Disabled
* **Upgrade Policy**: Standard
* **Cluster IAM Role**: arn:aws:iam::564507210442:role/EKSClusterRole
* **Admin Access**: Allowed
* **Authentication Mode**: EKS API
* **ARC Zonal Shift**: Disabled

### Networking

* **VPC**: vpc-003bdfebf2642ae4b
* **IP Family**: IPv4
* **Subnets**:
    * subnet-0334569bb7bef9234
    * subnet-0869aca9c67939a9e
* **Endpoint Access**: Public and private
* **Public Access CIDR**: 0.0.0.0/0

### Control Plane Logging

* API server
* Audit
* Authenticator
* Controller manager
* Scheduler

### Add-ons Configuration

|Add-on Name	|Type	|Version	|Service Account	|IAM Role	|
|---	|---	|---	|---	|---	|
|amazon-cloudwatch-observability	|observability	|v4.0.0-eksbuild.1	|cloudwatch-agent	|AmazonEKSPodIdentityAmazonCloudWatchObservabilityRole	|
|---	|---	|---	|---	|---	|
|coredns	|networking	|v1.11.4-eksbuild.2	|-	|-	|
|eks-node-monitoring-agent	|observability	|v1.2.0-eksbuild.1	|-	|-	|
|eks-pod-identity-agent	|security	|v1.3.4-eksbuild.1	|-	|-	|
|external-dns	|networking	|v0.16.1-eksbuild.2	|external-dns	|AmazonEKSPodIdentityExternalDNSRole	|
|kube-proxy	|networking	|v1.32.0-eksbuild.2	|-	|-	|
|metrics-server	|observability	|v0.7.2-eksbuild.3	|-	|-	|
|vpc-cni	|networking	|v1.19.2-eksbuild.1	|aws-node	|Not set	|

