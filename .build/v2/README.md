Declarative Test Suite Configuration
-------------------------------------------

Pipeline and test suite configurations are declarative so other CI implementations can build 
durable, reactive systems based on changes to the upstream OSS C* CI. Additions to `jobs.cfg` and 
`pipelines.cfg` can be picked up programmatically by CI implementations 
without requiring human intervention.

Concepts
---------------------

### Pipeline
A [pipeline](cassandra_ci.yaml) is a collection of jobs. For a given pipeline to be considered 
successful,
all
jobs listed in the pipeline must run to completion without error using the constraints, commands,
and environment specified for the job in the config.

### Job
A [job](jobs.yaml) contains a collection of parameters that inform a CI system on both what needs to 
run, how to run it, and the constraints of the environment in which it should execute. We 
provide these limits to reflect what's available in our reference ASF CI implementation so other 
CI environments are able to limit themselves to our resourcing upstream and thus not destabilize 
ASF CI.

Examples of jobs include unit tests, python dtests, in-jvm dtests, etc.

Jobs include the following parameters:

* `parent:` Another job defined in the file this job inherits parameters from, potentially 
  overwriting any declared in duplication
* `description:` Text based description of this job's purpose
* `cmd:` The command a shell should run to execute the test job
* `testlist:` A command that will create a text file listing all the test files to be run for 
  this 
  suite
* `env:` Space delimited list of environment variables to be set for this suite. Duplicates for 
  params are allowed and later declarations should supercede former.
* `cpu:` Max cpu count allowed for a testing suite
* `memory:` Max memory (in GB) allowable for a suite
* `storage:` Max allowable storage (in GB) allowable for a suite to access

Jobs can be split up and parallelized in whatever manner best suits the environment in which they're
orchestraed.

Configuration Files
---------------------

[pipelines.cfg](./cassandra_ci.yaml): Contains pipelines for CI jobs for Apache Cassandra

[jobs.cfg](./jobs.yaml): Contains reference CI jobs for Apache Cassandra

Existing Pipelines
---------------------

As outlined in the `pipelines.cfg` file, we primarily have 3 pipelines:
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

2. Add a new entry to [jobs.cfg](./jobs.yaml). For example:
```
job:my-new-job
    parent:base
    description:new test suite that does important new things
    cmd:ant new_job_name
    testlist:find test/new_test_type -name '*Test.java' | sort
    memory:12
    cpu:4
    storage:20
    env:PARAM_ONE=val1 PARAM_TWO=val2 PARAM_THREE=val3
    env:PARAM_FOUR=val4 PARAM_FIVE=val5
```

**NOTE**:

You will also need to ensure the necessary values exist in [build.xml](../../build.xml) (timeouts, 
etc).
For now, there is duplication between the declarative declaration of test suites here and `build.
xml`

Building a Testing Environment
-------------------------------------
[ci_config_parser.sh](./ci_config_parser.sh) contains several methods to parse out pipelines, jobs, 
and 
job parameters:

* `populate_pipelines`: populates a global array named `pipelines` with the names of all valid 
  pipelines from the given input file
* `populate_jobs`: populates all the required jobs for a given pipeline. Useful for determining 
  / breaking down and iterating through jobs needed for a given pipeline
* `parse_job_params`: populates some key global variables (see details in [ci_config_parser.sh](.
  /ci_config_parser.sh) that can be used to build out constraints, commands, and details in a 
  programmatic CI pipeline config builder.

The workflow for building CI programmatically from the config might look something like this:
* `populate_pipelines` to determine what pipelines you need to build out
* For each pipeline:
   1. `populate_jobs` to determine which jobs you need to write out config for
   2. for each job:
      1. `clear_job_params` to ensure nothing is left over from previous runs
      2. `parse_job_params` to set up the params needed for the job
      2. Write out the current job's params in whatever CI config format you're using in your 
         env (circle, jenkinsfile, etc)

As new entries are added to [pipelines.cfg](./cassandra_ci.yaml) and [jobs.cfg](./jobs.yaml), your 
scripts should pick those up and integrate them into your configuration environment.

Testing the in-tree config parsing scripts
---------------------------------------------
Currently testing is manual on the first addition of this declarative structure. As we integrate 
it into our reference CI, we will integrate testing in as a new target.

To run tests, execute [test_config.sh](./test/test_config.sh) from a terminal and inspect the 
output.