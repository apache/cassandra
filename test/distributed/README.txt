Distributed Test Harness


Sub-project description
-----------------------

A distributed test harness that deploys a cluster to a cloud provider,
via Apache Whirr, runs tests against that cluster, then tears down
the deployed cluster.

Requirements
------------
  * A cloud provider account. [see: http://incubator.apache.org/whirr/]


Getting started
---------------

First, setup an account w/ a supported cloud provider.  Then, refer to
the Whirr documentation for configuration instructions.  Refer to:
    * http://incubator.apache.org/whirr/quick-start-guide.html

Setup your personal whirr configuration properties.  The shared whirr
configuration is located at:
    * test/resources/whirr-default.properties

An example EC2/S3 whirr configuration would be:
###############################################
whirr.cluster-user=[username]
whirr.provider=aws-ec2
whirr.location-id=us-west-1
whirr.image-id=us-west-1/ami-16f3a253
whirr.hardware-id=m1.large
whirr.identity=[EC2 Access Key ID]
whirr.credential=[EC2 Secret Access Key]
whirr.private-key-file=${sys:user.home}/.ssh/id_rsa
whirr.public-key-file=${sys:user.home}/.ssh/id_rsa.pub
whirr.run-url-base=http://hoodidge.net/scripts/
whirr.blobstore.provider=aws-s3
whirr.blobstore.container=cassandratests
###############################################

The distributed tests are located in:
    * test/distributed

Run the tests via ant:
    * ant distributed-test -Dwhirr.config=my-whirr.properties

The ant target will:
    * download extra dependencies via Apache Ivy
    * compile the distributed tests
    * push the local working copy to a blobstore to fetch from the test nodes
    * deploy a cluster via Apache Whirr
    * run the distributed tests against the cluster
    * tear down the deployed cluster


