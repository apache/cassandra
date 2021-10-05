To run jvm upgrade tests first checkout each version and build it's dtest-jar target. Keep those artifacts, you'll need them to run the upgrade tests.
Remeber to recreate the dtest-jar if you change some code, as the tests exercises the jars, not the .java code

  cd cassandra
  mkdir dtest_jars
  for branch in cassandra-2.2 cassandra-3.0 cassandra-3.11 trunk (all versions you'll need); do
    git checkout $branch
    git clean -xdff
    ant realclean
    ant jar dtest-jar
    cp build/dtest*.jar dtest_jars
  git checkout the_branch_under_test
  ant realclean
  ant jar
  cp dtest_jars/* build
  You can test now