<!---
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at
 
     http://www.apache.org/licenses/LICENSE-2.0
 
 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
-->

## Injecting hooks dynamically in testing

The testing infrastructure is equipped with Byteman, which allows to
inject hooks into the running code. In other words, at any point in
the test, you can tell the particular nodes, to do something at exact
location in the code - like you would set a conditional breakpoint
and when the debugger breaks, you can check something or perform some
additional actions. In short, it is particularly useful to:

- synchronizing the test code and the nodes
- synchronizing the nodes between each other
- running actions step by step
- counting invocations
- tracing invocations
- force throwing exceptions
- and combinations of the above...

Here you can find a short introduction and some examples how can it be
used.

### Basics

Byteman works on rules which are the units defining what, when, and
where should be invoked. That is, a single rule includes: the invoke
point, bindings, run condition and actions.

**Invoke point** is a location in the code where the actions should
be hooked. Usually it is a class and method, but it can be specified
more precisely, like - entering method, exiting method, invoking some
method inside, particular line of code and so on.

**Bindings** are just some constant definitions which can be used then
in run condition and actions.

**Run condition** is a logical expression which determines whether
the rule should be invoked.

**Actions** are the statements to be invoked.

In DSE, we have `Rule` class which reflects a single Byteman rule.
It can be build by providing rule script directly or by providing
action and invoke point builders.

In DSE, we also have `Injection` class which groups multiple rules
which can be injected / enabled / disabled as a unit.

#### Creating Injection

```java
ActionBuilder actionBuilder = ActionBuilder.newActionBuilder()
    .actions().doThrow(newExpression(RuntimeException.class, "Induced exception"))
    .conditions().when(expression("System.currentTimeMillis() % 2 == 0"))
    .toActionBuilder();

InvokePointBuilder invokePointBuilder = new InvokePointBuilder()
    .atEntry().onClass(LeasePlugin.class).onMethod("setupSchema");

Injection injection = new Injection(
    "myInjection",
    new Rule[]{ Rule.newRule("rule1", actionBuilder, invokePointBuilder) });
```

#### Installing injection
Send all provided injections to a specified node:
```java
DseTestRunner.sendInjectionToNode(1, injection1, injection2, ...);
```

Install injections at bootstrap
```java
DseTestRunner.startNode(1,
    DseYamlBuilder.newInstance(),
    CassandraYamlBuilder.newInstance(),
    DseNode.Type.CASSANDRA,
    new Injection[] {injection1, injection2},
    true);
```

or

```java
DseNode node = DseTestRunner.getOrCreateNode(...);
node.setBootstrapInjectionRules(injection1, injection2, ...);
```

#### Disabling injection
Disable all rules included in injection:
```java
injection.disable();
```

#### Enabling injection
Reenable all rules included in injection:
```java
injection.enable();
```

#### Unloading injections
Unload all injections from specified nodes:
```java
DseTestRunner.removeAllInjectionsFromNodes(1, 2, 3);
```

The above injection will make DSE to throw `RuntimeException` when
entering `LeasePlugin.setupSchema` method with the probability of 50%.

### Predefined injections
In general, `CommonInjections` class includes builder for various
commonly used injections. Internally, the rules often use Hazelcast
for distributed latches, locks, and counters. However, it is all
hidden and you do no need to bother. The provided builders allow to
add multiple actions and invoke points easily, automatically
generating cross product of appropriate rules.

It is usually handy to add some imports to make the code less
wordy:

```java
import com.datastax.bdp.test.ng.inject.CommonInjections;

import static com.datastax.bdp.test.ng.inject.ActionBuilder.*;
import static com.datastax.bdp.test.ng.inject.InvokePointBuilder.*;
import static com.datastax.bdp.test.ng.inject.Expression.*;
```

#### Counter
Creates an injection along with a distributed counter. It increments
the counter whenever an invoke point is reached. You can add multiple
invoke points and you can also bundle additional actions - for example,
throw an exception and count how many times it happened.

For example you may want to count how many times a query was processed
on all nodes:

```java
CommonInjections.Counter queryCounter = CommonInjections.newCounter("queryCounter")
        .add(newInvokePoint().onClass(DseQueryHandler.class).onMethod("process").atEntry())
        .build();
// ...
queryCounter.injectIntoNodes(1, 2, 3);
// ...
assertEquals(5, queryCounter.get());
```

Another example is to count how many times `LeasePlugin.setupSchema`
will be attempted if it throws `UnavailableException` before the node
eventually die:

```java
CommonInjections.Counter retriesCounter = CommonInjections.newCounter("retriesCounter")
        .add(newInvokePoint().onClass(LeasePlugin.class).onMethod("setupSchema").atExit())
        .add(newActionBuilder().actions().doThrow(newInstance(UnavailableException.class).args(clazz(ConsistencyLevel.class).method("THREE"), 3, 1)))
        .build();

DseNode node = getOrCreateNode(1, DseNode.Type.CASSANDRA);
node.setBootstrapInjectionRules(retriesCounter);
node.start();

// ...
assertEquals(10, retriesCounter.get());
```

#### Barrier
Creates an injection with a distributed barrier awaiting for a defined
number of parties to reach it (including test node). It can be used to
synchronize multiple nodes - for example, you can make three nodes
synchronize on entering `LeasePlugin.setupSchema`:

```java
CommonInjections.Barrier barrier = CommonInjections.newBarrier("setupSchemaBarrier", 3, false)
        .add(newInvokePoint().onClass(LeasePlugin.class).onMethod("setupSchema").atEntry())
        .build();

DseNode node1 = getOrCreateNode(1, DseNode.Type.CASSANDRA).setBootstrapInjectionRules(barrier);
DseNode node2 = getOrCreateNode(2, DseNode.Type.CASSANDRA).setBootstrapInjectionRules(barrier);
DseNode node3 = getOrCreateNode(3, DseNode.Type.CASSANDRA).setBootstrapInjectionRules(barrier);

node1.start(false);
node2.start(false);
node3.start(false);

node1.waitForReady();
node2.waitForReady();
node3.waitForReady();
```

With barrier, you can also create more sophisticated synchronization
points. As you know, barrier a little bit similar to the count down
latch. When an execution arrives at the barrier, the number of parties
the barrier is waiting for is decremented and then it awaits until the
latch is zeroed. By normal a barrier does both things together. However
here you can separate them and pin to different invoke points so that
the execution awaiting in one place can be released by the execution in
the other place. For the purpose you create two barrier injections with
the same name. You can see this in [DbRollupReportingTest](dse-core/src/test/java/com/datastax/bdp/reporting/snapshots/db/inject/DbRollupReportingTest.java):
In this test, the count down part, which is non-blocking is pinned
to exit of `PeriodicUpdateTask.run`, while await part is pinned to
entry of `ClusterInfoRollupTask.doRollup`:

```java
private static final Barrier updateEnd = CommonInjections.newBarrierCountDown("updateToRollupSync", TOTAL_NODES, false)
        .add(newInvokePoint().onClassMethod(NodeSnapshotPlugin.class, "PeriodicUpdateTask", "run").atExit())
        .build();

private static final Barrier rollupStart = CommonInjections.newBarrierAwait("updateToRollupSync", TOTAL_NODES, false)
        .add(newInvokePoint().onClass(ClusterInfoRollupTask.class).onMethod("doRollup").atEntry())
        .build();
```

#### Times
Creates an injection which allows to invoke a defined action for
a defined number of times. For example, you may want to induce
`LeasePlugin.setupSchema` to throw `UnavailableException` for 3 times
and then eventually pass:

```java
CommonInjections.Times setupSchemaFailure = CommonInjections.newTimes("setupSchemaFailure", 3)
        .add(newInvokePoint().onClass(LeasePlugin.class).onMethod("setupSchema").atEntry())
        .add(newActionBuilder().actions().doThrow(newInstance(UnavailableException.class).args(clazz(ConsistencyLevel.class).method("THREE"), 3, 1)))
        .build();

DseNode node = getOrCreateNode(1, DseNode.Type.CASSANDRA);
node.setBootstrapInjectionRules(setupSchemaFailure);
node.start();
```

#### Step
Step is just a double barrier. It allows execute the code stopping at
the defined location each time it is reached. For example, you may want
to stop the execution of each node before entering into
`LeasePlugin.setupSchema` and wait for it to happen in the test code.
Then, do some changes in the schema from the test code, and resume
everything. This is exactly why you may want to use `Step`. In the
following example we want to stop 2 nodes before `setupSchema`:

```java
// the number of parties is 3 because it includes 2 nodes and the test worker
CommonInjections.Step step = CommonInjections.newStep("setupSchema", 3, false)
        .add(newInvokePoint().onClass(LeasePlugin.class).onMethod("setupSchema").atEntry())
        .build();

DseNode node1 = getOrCreateNode(1, DseNode.Type.CASSANDRA).setBootstrapInjectionRules(step);
DseNode node2 = getOrCreateNode(2, DseNode.Type.CASSANDRA).setBootstrapInjectionRules(step);
node1.start(false);
node2.start(false);

step.await();

// do some stuff while nodes are still waiting before entering setupSchema

step.resume();

node1.waitForReady();
node2.waitForReady();
```

#### Pause
Injects an additional pause into a defined location. For example, you
may want to slow down executing queries:

```java
Injection queryPause = CommonInjections.newPause("queryPause", 2000)
        .add(newInvokePoint().onClass(DseQueryHandler.class).onMethod("process").atEntry())
        .build();

// ...
sendInjectionToNode(1, queryPause);
// ...
```

#### Trace
Trace allows to put a message into standard out, standard err or a
particular file whenever an execution reaches a particular location.
Imagine this as adding some logging at runtime, which can be done even
against classes from third party jars you cannot tweak manually.

For example, you may want to know when a keyspace and a table is added
to Cassandra schema. You can log it to standard out:

```java
Injection setupSchemaTrace = CommonInjections.newTraceToStdOut()
        .add(newInvokePoint().onClass(Schema.class).onMethod("addTable"))
        .add(newInvokePoint().onClass(Schema.class).onMethod("addKeyspace"))
        .traceln(expr(quote("Adding to schema: ")).append("+").append(arg(1)))
        .build();

// ...
setupSchemaTrace.injectIntoNodes(1, 2, 3);
```

or into a file:

```
Injection setupSchemaTrace = CommonInjections.newTrace("schemaTrace", "schemaTrace.txt")
        .add(newInvokePoint().onClass(Schema.class).onMethod("addTable"))
        .add(newInvokePoint().onClass(Schema.class).onMethod("addKeyspace"))
        .traceln(expr(quote("Adding to schema: ")).append("+").append(arg(1)))
        .build();

// ...
setupSchemaTrace.injectIntoNodes(1, 2, 3);
```

### End notes

#### Byteman debugging
Whenever the test doesn't work as you expected, you may enable Byteman
verbose output, so that you can see if everything went ok with the
defined rules. For example, Byteman does not throw any exception if
there is a syntax error in the rule script. In order to see that, and
other useful information, just add `bytemanVerbose` property to the
test command:

```./gradlew test -PbytemanVerbose```

#### Several rules for the same method
If there are more rules for the same method, you should inject them
together because otherwise it may happen that some of them will not be
invoked (silently, without error message). Thus, send them in the
following way:

```java
DseTestRunner.sendInjectionToNode(nodeId, injection1, injection2);
```

#### Byteman rule language
If you need implement your own rule, you can refer to Byteman rule
language guide [here](https://github.com/bytemanproject/byteman/blob/master/docs/asciidoc/src/main/asciidoc/chapters/Byteman-Rule-Language.adoc).

#### Creating custom injections
Use Hazelcast to store objects you want to access from any test node
and test worker. Hazelcast offers distributed locks, maps, counters,
latches and many more. There a convenience class [Hazelcast](dse-core/src/test/java/com/datastax/bdp/test/ng/Hazelcast)
which allows to easily obtain either Hazelcast server or client
instance which are configured and ready to use. Remember to use a server
instance in the test code and a client instance for what you are doing
in the injection. You can also see how the common injections are
implemented.
