---
name: cassandra-injvm-dtest
version: "1.0.0"
description: Comprehensive guide for writing Apache Cassandra in-JVM distributed tests (dtests). Use when creating tests that simulate multi-node Cassandra clusters within a single JVM for faster integration testing. Covers cluster creation (single-node, multi-node, multi-datacenter), configuration (all cassandra.yaml parameters, features, network topology), instance lifecycle (startup/shutdown/restart), query execution, message filtering for failure scenarios, running code on instances, ClusterUtils utilities, and debugging classloader-related issues (serialization failures, same-class-different-classloader problems).
---

# Apache Cassandra In-JVM DTest Writer

## Overview

Write comprehensive distributed tests for Apache Cassandra using the in-JVM dtest framework. This framework enables multi-node cluster simulation within a single JVM using isolated classloaders, providing fast and reliable integration testing without actual network communication.

## Core Concepts

### Framework Architecture

The in-JVM dtest framework uses **classloader isolation** to run multiple Cassandra instances in one JVM:

- **Shared ClassLoader**: Common classes (primitives, APIs, config objects, byte buffers)
- **Per-Instance ClassLoader**: Isolated Cassandra internals (DatabaseDescriptor, singletons, all C* code)
- **IsolatedExecutor**: Marshals serialized lambdas across classloader boundaries
- **Message Interception**: Inter-node messaging hooked at MessagingService level (no actual networking)

### Key Classes

| Class | Purpose | Location |
|-------|---------|----------|
| `Cluster` | Main entry point for creating clusters | `org.apache.cassandra.distributed.Cluster` |
| `AbstractCluster` | Core cluster management | `org.apache.cassandra.distributed.impl.AbstractCluster` |
| `Instance` | Individual Cassandra node | `org.apache.cassandra.distributed.impl.Instance` |
| `InstanceConfig` | Node configuration | `org.apache.cassandra.distributed.impl.InstanceConfig` |
| `ClusterUtils` | Utility methods | `org.apache.cassandra.distributed.shared.ClusterUtils` |
| `ICoordinator` | Query execution interface | API package |
| `MessageFilters` | Message filtering/dropping | `org.apache.cassandra.distributed.impl.MessageFilters` |

## Quick Start

### Basic Cluster Setup

```java
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;

// Create and start a 3-node cluster
try (Cluster cluster = Cluster.build(3).start()) {
    // Create keyspace
    cluster.schemaChange("CREATE KEYSPACE test WITH replication = " +
                        "{'class': 'SimpleStrategy', 'replication_factor': 2}");

    // Create table
    cluster.schemaChange("CREATE TABLE test.users (id int PRIMARY KEY, name text)");

    // Execute query
    cluster.coordinator(1).execute("INSERT INTO test.users (id, name) VALUES (?, ?)",
                                  ConsistencyLevel.QUORUM, 1, "Alice");

    // Read data
    Object[][] rows = cluster.coordinator(1).execute(
        "SELECT * FROM test.users WHERE id = ?",
        ConsistencyLevel.QUORUM, 1);
}
```

### Test Base Class Pattern

```java
import org.apache.cassandra.distributed.test.TestBaseImpl;

public class MyDTest extends TestBaseImpl {
    private static Cluster cluster;

    @BeforeClass
    public static void setupCluster() throws IOException {
        cluster = init(Cluster.build(3).start(), 2); // RF=2
    }

    @AfterClass
    public static void teardownCluster() {
        if (cluster != null)
            cluster.close();
    }

    @Test
    public void testSomething() {
        // Test implementation
    }
}
```

## Cluster Creation Patterns

### Pattern 1: Simple Single-Node Cluster

```java
Cluster cluster = Cluster.build(1).start();
```

### Pattern 2: Multi-Node Cluster with Configuration

```java
Cluster cluster = Cluster.build(3)
    .withConfig(config -> config
        .set("num_tokens", "256")
        .set("concurrent_reads", "64")
        .set("concurrent_writes", "64"))
    .start();
```

### Pattern 3: Multi-Datacenter Cluster

```java
Cluster cluster = builder()
    .withRacks(2, 1, 3)  // 2 DCs, 1 rack per DC, 3 nodes per rack
    .start();

// Or with explicit DC/rack naming
Cluster cluster = builder()
    .withDC("dc1", 3)
    .withDC("dc2", 3)
    .start();
```

### Pattern 4: Cluster with Features

```java
import static org.apache.cassandra.distributed.api.Feature.*;

Cluster cluster = Cluster.build()
    .withNodes(3)
    .withConfig(config -> config
        .with(GOSSIP)           // Enable gossip protocol
        .with(NETWORK)          // Enable inter-node networking
        .with(NATIVE_PROTOCOL)  // Enable CQL protocol
        .with(JMX))             // Enable JMX
    .start();
```

### Pattern 5: Custom Token Distribution

```java
import org.apache.cassandra.distributed.api.TokenSupplier;

Cluster cluster = Cluster.build()
    .withNodes(3)
    .withTokenSupplier(TokenSupplier.evenlyDistributedTokens(3))
    .start();
```

### Pattern 6: Dynamic Port Allocation

```java
Cluster cluster = Cluster.build(3)
    .withDynamicPortAllocation(true)
    .start();
```

## Configuration

### Available Configuration Parameters

Configuration is applied via `config.set(key, value)` or `config.with(Feature)`:

#### Network Configuration
```java
config.set("broadcast_address", "127.0.0.1")
      .set("listen_address", "127.0.0.1")
      .set("rpc_address", "127.0.0.1")
      .set("storage_port", "7000")
      .set("native_transport_port", "9042")
      .set("jmx_port", "7199");
```

#### Token Configuration
```java
config.set("num_tokens", "256")
      .set("initial_token", "0");
```

#### Memory Configuration
```java
config.set("memtable_heap_space", "512MiB")
      .set("key_cache_size", "50MiB")
      .set("counter_cache_size", "50MiB")
      .set("row_cache_size", "0MiB");
```

#### Compaction & Concurrency
```java
config.set("concurrent_compactors", "4")
      .set("concurrent_reads", "32")
      .set("concurrent_writes", "32")
      .set("concurrent_counter_writes", "32");
```

#### Commitlog Configuration
```java
config.set("commitlog_sync", "periodic")
      .set("commitlog_sync_period_in_ms", "10000")
      .set("commitlog_segment_size", "32MiB");
```

#### Authentication & Authorization
```java
config.set("authenticator", "PasswordAuthenticator")
      .set("authorizer", "CassandraAuthorizer")
      .set("role_manager", "CassandraRoleManager");
```

#### Accord Configuration (CEP-15)
```java
config.set("accord.enabled", "true")
      .set("accord.journal_directory", "/path/to/journal");
```

#### Data Directories
```java
// Data directories are arrays
config.set("data_file_directories", new String[]{"/path/to/data"})
      .set("commitlog_directory", "/path/to/commitlog")
      .set("hints_directory", "/path/to/hints")
      .set("saved_caches_directory", "/path/to/saved_caches");
```

## Instance Lifecycle Operations

### Starting Instances

```java
// Start with default settings
instance.startup();

// Start with system properties
ClusterUtils.start(instance, properties -> {
    properties.set(RING_DELAY, "5000");
    properties.set(BOOTSTRAP_SCHEMA_DELAY_MS, "5000");
});
```

### Stopping Instances

```java
// Graceful shutdown (blocking)
ClusterUtils.stopUnchecked(instance);

// Abrupt shutdown (simulates kill -9)
ClusterUtils.stopAbrupt(cluster, instance);

// Stop all instances
ClusterUtils.stopAll(cluster);

// Async shutdown
Future<?> shutdownFuture = instance.shutdown();
shutdownFuture.get(); // wait for completion
```

### Restarting Instances

```java
// Simple restart
ClusterUtils.restartUnchecked(instance);

// Restart with different configuration
instance.shutdown().get();
instance.config().set("concurrent_reads", "64");
instance.startup();
```

### Adding Instances

```java
// Add instance with same config as existing nodes
IInstance newInstance = ClusterUtils.addInstance(cluster);

// Add instance with custom config
IInstance newInstance = ClusterUtils.addInstance(cluster, config -> {
    config.set("auto_bootstrap", true);
});

// Add instance to specific DC/rack
IInstance newInstance = ClusterUtils.addInstance(cluster, "dc1", "rack1", config -> {
    config.set("num_tokens", "256");
});

// Start the new instance
newInstance.startup();
```

### Replacing Instances

```java
// Replace a failed node
IInstance replacement = ClusterUtils.replaceHostAndStart(cluster, failedInstance);

// Replace with custom properties
IInstance replacement = ClusterUtils.replaceHostAndStart(
    cluster,
    failedInstance,
    (inst, properties) -> {
        properties.set(RING_DELAY, "5000");
    }
);
```

## Running Code on Instances

### Pattern 1: Run Without Return Value

```java
cluster.get(1).runOnInstance(() -> {
    StorageService.instance.forceKeyspaceFlush("system");
});
```

### Pattern 2: Get Return Value (Sync)

```java
String version = cluster.get(1).callOnInstance(() -> {
    return FBUtilities.getReleaseVersionString();
});
```

### Pattern 3: Get Return Value (Async)

```java
Future<String> future = cluster.get(1).asyncCallOnInstance(() -> {
    return FBUtilities.getReleaseVersionString();
});
String version = future.get();
```

### Pattern 4: Pass Parameters

```java
// Parameters must be serializable
int result = cluster.get(1).callOnInstance(() -> {
    // Access Cassandra internals here
    return ClusterMetadata.current().directory.allAddresses().size();
});
```

### Pattern 5: Instance-to-Instance Operations

```java
// Get node ID from one instance, use it on another
int nodeId = ClusterUtils.getNodeId(cluster.get(1), cluster.get(2));

NodeId id = new NodeId(nodeId);
cluster.get(2).runOnInstance(() -> {
    StorageService.instance.cancelInProgressSequences(id);
});
```

## Query Execution

### Basic Queries

```java
// Execute without results
cluster.coordinator(1).execute(
    "INSERT INTO test.users (id, name) VALUES (?, ?)",
    ConsistencyLevel.QUORUM,
    1, "Alice"
);

// Execute with results
Object[][] rows = cluster.coordinator(1).execute(
    "SELECT * FROM test.users WHERE id = ?",
    ConsistencyLevel.QUORUM,
    1
);
```

### Using Query Results

```java
import org.apache.cassandra.distributed.api.SimpleQueryResult;

SimpleQueryResult result = cluster.coordinator(1).executeWithResult(
    "SELECT * FROM test.users",
    ConsistencyLevel.QUORUM
);

while (result.hasNext()) {
    Row row = result.next();
    int id = row.getInteger("id");
    String name = row.getString("name");
    System.out.println("User: " + id + ", " + name);
}
```

### Pagination

```java
SimpleQueryResult result = cluster.coordinator(1).executeWithPagingWithResult(
    "SELECT * FROM test.users",
    ConsistencyLevel.QUORUM,
    100  // page size
);
```

### Async Execution

```java
Future<SimpleQueryResult> future = cluster.coordinator(1).asyncExecuteWithResult(
    "SELECT * FROM test.users",
    ConsistencyLevel.QUORUM
);

SimpleQueryResult result = future.get();
```

### Query with Tracing

```java
import java.util.UUID;

UUID sessionId = UUID.randomUUID();
SimpleQueryResult result = cluster.coordinator(1).executeWithTracingWithResult(
    sessionId,
    "SELECT * FROM test.users",
    ConsistencyLevel.QUORUM
);

// Query tracing info from system_traces
cluster.coordinator(1).execute(
    "SELECT * FROM system_traces.sessions WHERE session_id = ?",
    ConsistencyLevel.ONE,
    sessionId
);
```

### Internal Queries (No Coordinator)

```java
// Execute directly on instance internals (bypasses coordinator)
SimpleQueryResult result = instance.executeInternalWithResult(
    "SELECT * FROM system.peers WHERE peer=?",
    expectedPeer.config().broadcastAddress().getAddress()
);
```

## Message Filtering for Failure Injection

### Pattern 1: Drop Messages Between Nodes

```java
// Drop all mutation requests from node 1 to node 2
cluster.filters()
    .inbound()
    .from(1).to(2)
    .verbs(Verb.MUTATION_REQ.id)
    .drop()
    .on();

// Verify timeout behavior
assertThatThrownBy(() ->
    cluster.coordinator(1).execute(
        "INSERT INTO test.users (id, name) VALUES (?, ?)",
        ConsistencyLevel.ALL, 1, "Alice"
    )
).isInstanceOf(WriteTimeoutException.class);

// Reset filters
cluster.filters().reset();
```

### Pattern 2: Drop All Verbs

```java
cluster.filters()
    .allVerbs()
    .from(1).to(2)
    .drop()
    .on();
```

### Pattern 3: Drop Outbound Messages

```java
cluster.filters()
    .outbound()
    .from(1)
    .verbs(Verb.READ_REQ.id)
    .drop()
    .on();
```

### Pattern 4: Custom Message Matching

```java
cluster.filters()
    .inbound()
    .from(1).to(2)
    .messagesMatching((from, to, message) -> {
        // Custom logic to match specific messages
        return message.verb() == Verb.MUTATION_REQ.id;
    })
    .drop()
    .on();
```

### Pattern 5: Temporary Filter Control

```java
IMessageFilters.Filter filter = cluster.filters()
    .inbound()
    .from(1).to(2)
    .verbs(Verb.MUTATION_REQ.id)
    .drop();

filter.on();  // Enable
// ... perform operations ...
filter.off(); // Disable
```

## ClusterUtils Reference

For comprehensive utilities, see [references/cluster_utils.md](references/cluster_utils.md).

### Most Commonly Used Methods

#### Lifecycle Management
```java
ClusterUtils.stopUnchecked(instance);
ClusterUtils.stopAbrupt(cluster, instance);
ClusterUtils.restartUnchecked(instance);
ClusterUtils.stopAll(cluster);
```

#### Instance Management
```java
ClusterUtils.addInstance(cluster, config -> {...});
ClusterUtils.replaceHostAndStart(cluster, toReplace);
ClusterUtils.start(instance, properties -> {...});
```

#### Ring & Gossip
```java
ClusterUtils.awaitRingJoin(cluster.get(1), newInstance);
ClusterUtils.awaitRingHealthy(cluster.get(1));
ClusterUtils.awaitGossipStatus(instance, target, "NORMAL");
ClusterUtils.assertInRing(instance, expectedInRing);
```

#### CMS & Metadata
```java
ClusterUtils.waitForCMSToQuiesce(cluster);
Epoch epoch = ClusterUtils.getCurrentEpoch(instance);
ClusterUtils.fetchLogFromCMS(instance, awaitedEpoch);
```

#### Directories & Files
```java
List<File> dirs = ClusterUtils.getDataDirectories(instance);
File commitlog = ClusterUtils.getCommitLogDirectory(instance);
File hints = ClusterUtils.getHintsDirectory(instance);
```

## Schema Management

### Creating Keyspaces

```java
cluster.schemaChange("CREATE KEYSPACE test WITH replication = " +
                    "{'class': 'SimpleStrategy', 'replication_factor': 2}");

// Multi-DC keyspace
cluster.schemaChange("CREATE KEYSPACE test WITH replication = " +
                    "{'class': 'NetworkTopologyStrategy', " +
                    "'dc1': 2, 'dc2': 2}");
```

### Creating Tables

```java
cluster.schemaChange("CREATE TABLE test.users (" +
                    "id int PRIMARY KEY, " +
                    "name text, " +
                    "email text)");

// With clustering
cluster.schemaChange("CREATE TABLE test.events (" +
                    "user_id int, " +
                    "event_time timestamp, " +
                    "event_type text, " +
                    "PRIMARY KEY (user_id, event_time)) " +
                    "WITH CLUSTERING ORDER BY (event_time DESC)");
```

### Waiting for Schema Agreement

```java
// Manual wait
try (Cluster.SchemaChangeMonitor monitor = cluster.new SchemaChangeMonitor()) {
    monitor.startPolling();
    cluster.schemaChange("CREATE TABLE ...");
    monitor.waitForCompletion();
}

// Using ClusterUtils
ClusterUtils.awaitGossipSchemaMatch(cluster);
```

## Monitoring & Observables

### Log Watching

```java
// Mark position and wait for log message
long mark = instance.logs().mark();
// ... perform operation ...
instance.logs().watchFor(mark, "Bootstrap completed");

// Grep logs
List<String> errors = instance.logs().grep("ERROR").getResult();
```

### Nodetool Integration

```java
import org.apache.cassandra.distributed.api.NodeToolResult;

NodeToolResult result = instance.nodetoolResult("status");
result.asserts().success();
String output = result.getStdout();

// Common nodetool commands
instance.nodetool("flush");
instance.nodetool("repair");
instance.nodetool("compact");
instance.nodetool("decommission");
```

### Ring Information

```java
import org.apache.cassandra.distributed.shared.ClusterUtils.RingInstanceDetails;

List<RingInstanceDetails> ring = ClusterUtils.ring(instance);
for (RingInstanceDetails details : ring) {
    System.out.println("Address: " + details.getAddress());
    System.out.println("Status: " + details.getStatus());   // Up/Down
    System.out.println("State: " + details.getState());     // Normal/Leaving/Joining
    System.out.println("Token: " + details.getToken());
}
```

### Gossip Information

```java
Map<String, Map<String, String>> gossip = ClusterUtils.gossipInfo(instance);
for (Map.Entry<String, Map<String, String>> entry : gossip.entrySet()) {
    String address = entry.getKey();
    Map<String, String> state = entry.getValue();
    System.out.println(address + " -> " + state.get("STATUS"));
}
```

## Classloader Issues & Debugging

### Understanding Classloader Isolation

The in-JVM dtest framework uses multiple classloaders for isolation:
- **Test ClassLoader**: Where your test code runs
- **Shared ClassLoader**: Primitives, APIs, config objects
- **Instance ClassLoaders**: Per-instance Cassandra code (isolated)

### Common Classloader Issues

#### Issue 1: Same Class, Different ClassLoaders

**Symptom**: `ClassCastException` even though classes appear identical

```java
// WRONG - This will fail
cluster.get(1).runOnInstance(() -> {
    Token token = new Murmur3Partitioner.LongToken(0); // Loaded in test classloader
    // Try to use token with instance internals -> ClassCastException
});

// CORRECT - Create objects inside runOnInstance
cluster.get(1).runOnInstance(() -> {
    Token token = new Murmur3Partitioner.LongToken(0); // Loaded in instance classloader
    // Now token is in correct classloader
});
```

#### Issue 2: Serialization Failures

**Symptom**: `NotSerializableException` or `ClassNotFoundException`

```java
// WRONG - Non-serializable lambda capture
String localVar = "test";
cluster.get(1).runOnInstance(() -> {
    System.out.println(localVar); // May fail if localVar isn't Serializable
});

// CORRECT - Only pass serializable primitives
final String localVar = "test"; // String is Serializable
cluster.get(1).runOnInstance(() -> {
    System.out.println(localVar);
});
```

**Lambdas are serialized** and transferred across classloaders. Only capture:
- Primitives (int, long, String, etc.)
- Serializable objects marked with `@Shared`
- Nothing else from test classloader

#### Issue 3: Static State Not Shared

**Symptom**: Static fields don't reflect changes across instances

```java
// Each instance has its own DatabaseDescriptor, StorageService, etc.
// Static state is NOT shared between instances

// To coordinate across instances, use:
// 1. CQL queries to share data
// 2. Message passing
// 3. Instance-specific operations
```

### Debugging Techniques

#### Technique 1: Print Classloader Information

```java
cluster.get(1).runOnInstance(() -> {
    System.out.println("ClassLoader: " +
        Thread.currentThread().getContextClassLoader());
    System.out.println("Token class loader: " +
        Token.class.getClassLoader());
});
```

#### Technique 2: Transfer Objects Explicitly

```java
import org.apache.cassandra.distributed.impl.IsolatedExecutor;

// Transfer object to instance classloader
Serializable transferred = instance.transfer(mySerializableObject);
```

#### Technique 3: Use @Shared and @Isolated Annotations

```java
// In your test code
import org.apache.cassandra.utils.Shared;
import org.apache.cassandra.utils.Isolated;

@Shared  // Loaded in shared classloader
public class MySharedClass implements Serializable {
    // Can be shared across boundaries
}

@Isolated  // Each instance gets own copy
public class MyIsolatedClass {
    // Not shared
}
```

#### Technique 4: Check Serialization

```java
// Verify object can be serialized
try {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ObjectOutputStream oos = new ObjectOutputStream(baos);
    oos.writeObject(myObject);
    oos.close();
    System.out.println("Object is serializable");
} catch (NotSerializableException e) {
    System.out.println("Object is NOT serializable: " + e.getMessage());
}
```

### Best Practices for Classloader Safety

1. **Keep lambdas simple**: Minimize captured variables
2. **Use primitives**: Pass int, String, long instead of complex objects
3. **Create objects inside runOnInstance**: Don't create in test, pass to instance
4. **Avoid static state**: Don't rely on static fields across instances
5. **Use coordinator for data sharing**: Query data rather than passing objects

## Advanced Patterns

### Pattern: Pausing & Resuming CMS Operations

```java
// Pause before committing at specific epoch
Callable<Epoch> pauseHandle = ClusterUtils.pauseBeforeCommit(
    cmsInstance,
    transformation -> transformation instanceof AddNode
);

// Wait for pause
Epoch epoch = pauseHandle.call();

// Do something while paused
// ...

// Resume
ClusterUtils.unpauseCommits(cmsInstance);
```

### Pattern: Pausing Enactment

```java
// Pause before enacting specific epoch
Callable<Void> pauseHandle = ClusterUtils.pauseBeforeEnacting(instance, epoch);

// Wait for pause
pauseHandle.call();

// Do something while paused
// ...

// Resume
ClusterUtils.unpauseEnactment(instance);
```

### Pattern: Testing with Log Monitoring

```java
ClusterUtils.runAndWaitForLogs(
    () -> {
        cluster.coordinator(1).execute("INSERT INTO test.users VALUES (1, 'Alice')",
                                      ConsistencyLevel.QUORUM);
    },
    "Mutation applied",
    cluster.get(1), cluster.get(2), cluster.get(3)
);
```

### Pattern: Custom Instance Initialization

```java
Cluster cluster = Cluster.build()
    .withNodes(3)
    .withInstanceInitializer((cl, i) -> {
        // Custom initialization logic
        System.out.println("Initializing instance " + i);
    })
    .start();
```

### Pattern: Testing Node Replacement

```java
// Original node
IInvokableInstance original = cluster.get(2);

// Stop original
ClusterUtils.stopAbrupt(cluster, original);

// Replace with new node
IInvokableInstance replacement = ClusterUtils.replaceHostAndStart(
    cluster,
    original,
    (inst, properties) -> {
        properties.set(RING_DELAY, "5000");
    }
);

// Verify replacement joined
ClusterUtils.awaitRingJoin(cluster.get(1), replacement);
```

### Pattern: Testing Decommission

```java
IInvokableInstance leaving = cluster.get(3);

boolean success = ClusterUtils.decommission(leaving);
Assert.assertTrue("Decommission failed", success);

// Verify node left
ClusterUtils.assertNotInRing(cluster.get(1), leaving);
```

### Pattern: Multi-DC Token Ranges

```java
List<ClusterUtils.Range> ranges = ClusterUtils.getPrimaryRanges(
    cluster.get(1),
    "test"  // keyspace
);

for (ClusterUtils.Range range : ranges) {
    System.out.println("Range: " + range.left() + " to " + range.right());
}
```

## Complete Test Examples

### Example 1: Basic Read/Write Test

```java
public class BasicReadWriteTest extends TestBaseImpl {
    @Test
    public void testReadWrite() throws IOException {
        try (Cluster cluster = Cluster.build(3).start()) {
            cluster.schemaChange("CREATE KEYSPACE test WITH replication = " +
                                "{'class': 'SimpleStrategy', 'replication_factor': 2}");
            cluster.schemaChange("CREATE TABLE test.users (id int PRIMARY KEY, name text)");

            // Write
            cluster.coordinator(1).execute(
                "INSERT INTO test.users (id, name) VALUES (?, ?)",
                ConsistencyLevel.QUORUM, 1, "Alice");

            // Read
            Object[][] rows = cluster.coordinator(2).execute(
                "SELECT * FROM test.users WHERE id = ?",
                ConsistencyLevel.QUORUM, 1);

            Assert.assertEquals(1, rows.length);
            Assert.assertEquals(1, rows[0][0]);
            Assert.assertEquals("Alice", rows[0][1]);
        }
    }
}
```

### Example 2: Testing Write Timeout with Message Filtering

```java
public class WriteTimeoutTest extends TestBaseImpl {
    @Test
    public void testWriteTimeout() throws IOException {
        try (Cluster cluster = Cluster.build(3).start()) {
            cluster.schemaChange("CREATE KEYSPACE test WITH replication = " +
                                "{'class': 'SimpleStrategy', 'replication_factor': 3}");
            cluster.schemaChange("CREATE TABLE test.data (k int PRIMARY KEY, v int)");

            // Drop mutations to node 2 and 3
            cluster.filters().inbound()
                .to(2, 3)
                .verbs(Verb.MUTATION_REQ.id)
                .drop()
                .on();

            // This should timeout (can't reach QUORUM)
            try {
                cluster.coordinator(1).execute(
                    "INSERT INTO test.data (k, v) VALUES (?, ?)",
                    ConsistencyLevel.QUORUM, 1, 100);
                Assert.fail("Expected WriteTimeoutException");
            } catch (RuntimeException e) {
                Assert.assertTrue(e.getCause() instanceof WriteTimeoutException);
            }

            cluster.filters().reset();
        }
    }
}
```

### Example 3: Testing Node Addition

```java
public class NodeAdditionTest extends TestBaseImpl {
    @Test
    public void testAddNode() throws IOException {
        try (Cluster cluster = Cluster.build(2).start()) {
            cluster.schemaChange("CREATE KEYSPACE test WITH replication = " +
                                "{'class': 'SimpleStrategy', 'replication_factor': 2}");

            // Add and start new node
            IInvokableInstance newNode = ClusterUtils.addInstance(cluster, config -> {
                config.set("auto_bootstrap", true);
            });
            newNode.startup();

            // Wait for node to join
            ClusterUtils.awaitRingJoin(cluster.get(1), newNode);

            // Verify all nodes see the new node
            ClusterUtils.assertInRing(cluster.get(1), newNode);
            ClusterUtils.assertInRing(cluster.get(2), newNode);
        }
    }
}
```

### Example 4: Running Code on Instance

```java
public class InstanceCodeTest extends TestBaseImpl {
    @Test
    public void testRunOnInstance() throws IOException {
        try (Cluster cluster = Cluster.build(2).start()) {
            cluster.schemaChange("CREATE KEYSPACE test WITH replication = " +
                                "{'class': 'SimpleStrategy', 'replication_factor': 1}");
            cluster.schemaChange("CREATE TABLE test.data (k int PRIMARY KEY)");

            // Insert data
            cluster.coordinator(1).execute("INSERT INTO test.data (k) VALUES (1)",
                                          ConsistencyLevel.ONE);

            // Force flush on node 1
            cluster.get(1).runOnInstance(() -> {
                StorageService.instance.forceKeyspaceFlush("test");
            });

            // Verify flush happened by checking for sstables
            int sstableCount = cluster.get(1).callOnInstance(() -> {
                ColumnFamilyStore cfs = Keyspace.open("test")
                                               .getColumnFamilyStore("data");
                return cfs.getLiveSSTables().size();
            });

            Assert.assertTrue("Expected at least one sstable", sstableCount > 0);
        }
    }
}
```

### Example 5: Multi-DC Test

```java
public class MultiDCTest extends TestBaseImpl {
    @Test
    public void testMultiDC() throws IOException {
        try (Cluster cluster = builder()
                                  .withRacks(2, 1, 2)  // 2 DCs, 1 rack each, 2 nodes per rack
                                  .start()) {

            cluster.schemaChange("CREATE KEYSPACE test WITH replication = " +
                                "{'class': 'NetworkTopologyStrategy', " +
                                "'datacenter1': 2, 'datacenter2': 2}");
            cluster.schemaChange("CREATE TABLE test.data (k int PRIMARY KEY)");

            // Write from DC1
            cluster.coordinator(1).execute("INSERT INTO test.data (k) VALUES (1)",
                                          ConsistencyLevel.LOCAL_QUORUM);

            // Read from DC2
            Object[][] rows = cluster.coordinator(3).execute(
                "SELECT * FROM test.data WHERE k = 1",
                ConsistencyLevel.LOCAL_QUORUM);

            Assert.assertEquals(1, rows.length);
        }
    }
}
```

## Troubleshooting Guide

### Problem: Tests are flaky or hang

**Possible causes:**
1. Schema not fully agreed before operations
2. Insufficient timeout for async operations
3. Message filters not properly reset

**Solutions:**
```java
// Wait for schema agreement
ClusterUtils.awaitGossipSchemaMatch(cluster);

// Use longer timeouts for operations
future.get(30, TimeUnit.SECONDS);

// Always reset filters in finally block
try {
    cluster.filters().inbound().drop().on();
    // test code
} finally {
    cluster.filters().reset();
}
```

### Problem: ClassCastException with tokens or types

**Cause:** Object created in wrong classloader

**Solution:**
```java
// Create objects inside runOnInstance
cluster.get(1).runOnInstance(() -> {
    Token token = new Murmur3Partitioner.LongToken(0);
    // Use token here
});
```

### Problem: NoClassDefFoundError or ClassNotFoundException

**Cause:** Class not available in instance classloader

**Solution:** Ensure class is in Cassandra dependencies or marked `@Shared`

### Problem: Test times out waiting for ring/gossip

**Cause:** Network features not enabled

**Solution:**
```java
Cluster cluster = builder()
    .withConfig(c -> c.with(GOSSIP, NETWORK))
    .start();
```

### Problem: Port conflicts

**Cause:** Static port allocation

**Solution:**
```java
Cluster cluster = Cluster.build(3)
    .withDynamicPortAllocation(true)
    .start();
```

## Additional Resources

- **ClusterUtils**: See [references/cluster_utils.md](references/cluster_utils.md) for exhaustive utility method documentation
- **Advanced Patterns**: See [references/advanced_patterns.md](references/advanced_patterns.md) for CMS operations, Accord testing, and complex scenarios
- **Classloader Deep Dive**: See [references/classloader_guide.md](references/classloader_guide.md) for detailed classloader mechanics and debugging
