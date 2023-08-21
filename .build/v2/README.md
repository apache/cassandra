Declarative Test Suite Configuration
-------------------------------------------

    TODO

Concepts
---------------------

### Pipeline
A pipeline is a collection of jobs. For a given pipeline to be considered 
successful,
all
jobs listed in the pipeline must run to completion without error using the constraints, commands,
and environment specified for the job in the config.

### Job
A job contains a collection of parameters that inform a CI system on both what needs to 
run, how to run it, and the constraints of the environment in which it should execute. We 
provide these limits to reflect what's available in our reference ASF CI implementation so other 
CI environments are able to limit themselves to our resourcing upstream and thus not destabilize 
ASF CI.

Examples of jobs include unit tests, python dtests, in-jvm dtests, etc.

Jobs can include many parameters. Some highlights:

Jobs can be split up and parallelized in whatever manner best suits the environment in which they're
orchestraed.

Configuration Files
---------------------
[cassandra_ci.yaml](./cassandra_ci.yaml): Root default configuration for CI

[ci_function.sh](./ci_functions.sh): Canonical CI functions to run CI

[functions.sh](./functions.sh): Some helper functions to run CI

Existing Pipelines
---------------------

As outlined in the `pipelines:` array in the .yaml, we have 3 pipelines:
### pre-commit:
* must run and pass on the lowest supported JDK before a committer merges any code
### post-commit:
* will run on the upstream ASF repo after a commit is merged, matrixed across more axes and including configurations expected to fail or diverge only rarely
### nightly:
* run nightly. Longer term, infra, very stable areas of code.

Adding a new job to CI
---------------------
To add a new job to CI, you need to do 2 things:
1. Determine which pipeline it will be a part of. Add the job name to that pipeline (or create a
new pipeline with that job)

2. Add a new entry to the jobs: array with the details of your job

- JVM-single-node based example
```
  - <<: *env_jvm_base
    name: <my-new-jvm-job>
    resources: <any resource differences you might need>
    before: <any changes needed here to setup the env>
    run: <new_method_in_[ci_functions.sh]>  
    after: <any changes or custom stuff you need to run after>
```

- JVM-dtest based example
```
  - <<: *job_jvm_dtest_base
    name: <new_jvm_dtest_job>
    run: <new method in ci_functions.sh>
```

Building a Testing Environment
-------------------------------------

    TODO

Testing the in-tree config parsing scripts
---------------------------------------------

    TODO