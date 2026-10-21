# ClusterUtils Comprehensive Reference

Complete reference for `org.apache.cassandra.distributed.shared.ClusterUtils`.

## Lifecycle Management

### start()
```java
<I extends IInstance> I start(I inst, Consumer<WithProperties> fn)
<I extends IInstance> I start(I inst, BiConsumer<I, WithProperties> fn)
```
Start instance with system properties that are cleared after startup.

**Example:**
```java
ClusterUtils.start(instance, properties -> {
    properties.set(RING_DELAY, "5000");
    properties.set(BROADCAST_INTERVAL_MS, "30000");
});
```

### stopUnchecked()
```java
void stopUnchecked(IInstance i)
```
Stop instance in blocking manner, throwing runtime exceptions on failure.

### stopAbrupt()
```java
<I extends IInstance> void stopAbrupt(ICluster<I> cluster, I inst)
```
Simulate kill -9 by blocking all messages to/from node before shutdown.

### stopAll()
```java
<I extends IInstance> void stopAll(ICluster<I> cluster)
```
Stop all instances without cleaning cluster state.

### restartUnchecked()
```java
void restartUnchecked(IInstance instance)
```
Stop and restart instance in blocking manner.

## Instance Management

### addInstance()
```java
<I extends IInstance> I addInstance(AbstractCluster<I> cluster, Consumer<IInstanceConfig> fn)
<I extends IInstance> I addInstance(AbstractCluster<I> cluster)
<I extends IInstance> I addInstance(AbstractCluster<I> cluster, IInstanceConfig other, Consumer<IInstanceConfig> fn)
<I extends IInstance> I addInstance(AbstractCluster<I> cluster, String dc, String rack)
<I extends IInstance> I addInstance(AbstractCluster<I> cluster, String dc, String rack, Consumer<IInstanceConfig> fn)
```
Create and add new instance to cluster without starting it.

**Examples:**
```java
// Add with same config as existing nodes
IInstance inst = ClusterUtils.addInstance(cluster);

// Add with custom config
IInstance inst = ClusterUtils.addInstance(cluster, config -> {
    config.set("auto_bootstrap", true);
    config.set("num_tokens", "256");
});

// Add to specific DC/rack
IInstance inst = ClusterUtils.addInstance(cluster, "dc1", "rack1", config -> {
    config.set("concurrent_reads", "64");
});
```

### replaceHostAndStart()
```java
<I extends IInstance> I replaceHostAndStart(AbstractCluster<I> cluster, I toReplace)
<I extends IInstance> I replaceHostAndStart(AbstractCluster<I> cluster, I toReplace, Consumer<WithProperties> fn)
<I extends IInstance> I replaceHostAndStart(AbstractCluster<I> cluster, I toReplace, BiConsumer<I, WithProperties> fn)
<I extends IInstance> I replaceHostAndStart(AbstractCluster<I> cluster, I toReplace, BiConsumer<I, WithProperties> fn, Consumer<IInstanceConfig> configFn)
```
Create and start new instance that replaces an existing instance.

**Example:**
```java
IInvokableInstance replacement = ClusterUtils.replaceHostAndStart(
    cluster,
    failedNode,
    (inst, properties) -> {
        properties.set(RING_DELAY, "5000");
        properties.set(BROADCAST_INTERVAL_MS, "30000");
    },
    config -> {
        config.set("concurrent_compactors", "8");
    }
);
```

### startHostReplacement()
```java
<I extends IInstance> I startHostReplacement(I toReplace, I inst)
<I extends IInstance> I startHostReplacement(I toReplace, I inst, BiConsumer<I, WithProperties> fn)
```
Start instance with properties needed for host replacement.

## Token & Metadata

### getTokenMetadataTokens()
```java
List<String> getTokenMetadataTokens(IInvokableInstance inst)
```
Get all tokens from TokenMap as list of strings.

### getLocalTokens()
```java
Collection<String> getLocalTokens(IInvokableInstance inst)
```
Get tokens assigned to this instance.

### getTokens()
```java
List<String> getTokens(IInstance instance)
```
Get tokens from config (only works if configured, not learned/generated).

### getTokenCount()
```java
int getTokenCount(IInvokableInstance instance)
```
Get num_tokens from config.

### getPartitionerName()
```java
String getPartitionerName(IInstance instance)
```
Get configured partitioner name.

### getPrimaryRanges()
```java
List<Range> getPrimaryRanges(IInvokableInstance instance, String keyspace)
```
Get primary token ranges for instance for given keyspace.

**Example:**
```java
List<ClusterUtils.Range> ranges = ClusterUtils.getPrimaryRanges(
    cluster.get(1),
    "my_keyspace"
);
for (ClusterUtils.Range range : ranges) {
    System.out.println(range.left() + " to " + range.right());
}
```

## Cluster Metadata & CMS

### getClusterMetadataVersion()
```java
Epoch getClusterMetadataVersion(IInvokableInstance inst)
```
Get current cluster metadata epoch.

### getCurrentEpoch()
```java
Epoch getCurrentEpoch(IInvokableInstance inst)
```
Get current epoch from ClusterMetadata.

### getNextEpoch()
```java
Epoch getNextEpoch(IInvokableInstance inst)
```
Get next epoch from ClusterMetadata.

### maxEpoch()
```java
Epoch maxEpoch(ICluster<IInvokableInstance> cluster)
Epoch maxEpoch(ICluster<IInvokableInstance> cluster, int... nodes)
```
Find largest epoch across cluster or specified nodes.

### waitForCMSToQuiesce()
```java
void waitForCMSToQuiesce(ICluster<IInvokableInstance> cluster, int... cmsNodes)
void waitForCMSToQuiesce(ICluster<IInvokableInstance> cluster, Epoch awaitedEpoch, int... ignored)
void waitForCMSToQuiesce(ICluster<IInvokableInstance> cluster, Epoch awaitedEpoch, boolean fetchLogWhenBehind, int... ignored)
```
Wait for all nodes to reach same cluster metadata state.

**Example:**
```java
// Wait for all nodes to converge
ClusterUtils.waitForCMSToQuiesce(cluster);

// Wait for specific epoch
Epoch targetEpoch = ClusterUtils.maxEpoch(cluster);
ClusterUtils.waitForCMSToQuiesce(cluster, targetEpoch);
```

### fetchLogFromCMS()
```java
Epoch fetchLogFromCMS(IInvokableInstance inst, Epoch awaitedEpoch)
Epoch fetchLogFromCMS(IInvokableInstance inst, long awaitedEpoch)
```
Fetch cluster metadata log from CMS up to specified epoch.

### snapshotClusterMetadata()
```java
Epoch snapshotClusterMetadata(IInvokableInstance inst)
```
Trigger cluster metadata snapshot and return resulting epoch.

### getPeerEpochs()
```java
Map<String, Epoch> getPeerEpochs(IInvokableInstance requester)
```
Request current epochs from all peers in cluster.

### getCMSMembers()
```java
Set<String> getCMSMembers(IInvokableInstance inst)
```
Get set of CMS member addresses.

### getPeerDirectoryDebugStrings()
```java
List<String> getPeerDirectoryDebugStrings(IInvokableInstance inst)
```
Get debug strings for peer directory (one line per node).

### getTokenMapDebugStrings()
```java
List<String> getTokenMapDebugStrings(IInvokableInstance inst)
```
Get debug strings for token map.

### logTokenMapDebugString()
```java
void logTokenMapDebugString(IInvokableInstance inst)
```
Log token map debug info on instance.

### getDataPlacementDebugInfo()
```java
Map<String, List[]> getDataPlacementDebugInfo(IInvokableInstance inst)
```
Get placement debug info: for each keyspace, returns 2-element array with read and write replica lists.

### logDataPlacementDebugString()
```java
void logDataPlacementDebugString(IInvokableInstance inst, boolean byEndpoint)
```
Log data placement debug info.

## CMS Pausing & Control

### pauseBeforeCommit()
```java
Callable<Epoch> pauseBeforeCommit(IInvokableInstance cmsInstance, SerializablePredicate<Transformation> predicate)
```
Pause CMS before committing transformation matching predicate. Returns callable that waits for pause.

**Example:**
```java
Callable<Epoch> pauseHandle = ClusterUtils.pauseBeforeCommit(
    cmsNode,
    transformation -> transformation instanceof AddNode
);

// Wait for pause (blocks until predicate matches)
Epoch pausedAt = pauseHandle.call();

// Do something while paused
// ...

// Resume
ClusterUtils.unpauseCommits(cmsNode);
```

### getSequenceAfterCommit()
```java
Callable<Epoch> getSequenceAfterCommit(IInvokableInstance cmsInstance,
                                       SerializableBiPredicate<Transformation, Commit.Result> predicate)
```
Get epoch after transformation commits matching predicate.

### pauseBeforeEnacting()
```java
Callable<Void> pauseBeforeEnacting(IInvokableInstance instance, long epoch)
Callable<Void> pauseBeforeEnacting(IInvokableInstance instance, Epoch epoch)
```
Pause before enacting specific epoch. Returns callable that waits for pause.

**Example:**
```java
Callable<Void> pauseHandle = ClusterUtils.pauseBeforeEnacting(instance, targetEpoch);

// Wait for pause
pauseHandle.call();

// Perform operations while paused
// ...

// Resume
ClusterUtils.unpauseEnactment(instance);
```

### pauseAfterEnacting()
```java
Callable<Void> pauseAfterEnacting(IInvokableInstance instance, Epoch epoch)
```
Pause after enacting specific epoch.

### unpauseCommits()
```java
void unpauseCommits(IInvokableInstance instance)
```
Resume CMS commit processing.

### unpauseEnactment()
```java
void unpauseEnactment(IInvokableInstance instance)
```
Resume epoch enactment.

### clearAndUnpause()
```java
void clearAndUnpause(IInvokableInstance instance)
```
Clear all pause conditions and resume.

### dropAllEntriesBeginningAt()
```java
void dropAllEntriesBeginningAt(IInvokableInstance instance, Epoch epoch)
```
Add filter to drop all log entries at or after specified epoch.

### clearEntryFilters()
```java
void clearEntryFilters(IInvokableInstance instance)
```
Clear all entry filters.

## Ring & Gossip Monitoring

### ring()
```java
List<RingInstanceDetails> ring(IInstance inst)
```
Get ring from nodetool ring output.

**RingInstanceDetails fields:**
- `address`: Node address
- `rack`: Rack name
- `status`: Up/Down
- `state`: Normal/Leaving/Joining/Moving
- `token`: Token value

### assertInRing()
```java
List<RingInstanceDetails> assertInRing(IInstance instance, IInstance expectedInRing)
```
Assert target instance is in ring.

### assertNotInRing()
```java
List<RingInstanceDetails> assertNotInRing(IInstance instance, IInstance expectedInRing)
```
Assert target instance is NOT in ring.

### assertRingState()
```java
List<RingInstanceDetails> assertRingState(IInstance instance, IInstance expectedInRing, String state)
```
Assert target has specific state (Normal, Leaving, Joining, etc.).

### assertRingIs()
```java
List<RingInstanceDetails> assertRingIs(IInstance instance, IInstance... expectedInRing)
List<RingInstanceDetails> assertRingIs(IInstance instance, Collection<? extends IInstance> expectedInRing)
List<RingInstanceDetails> assertRingIs(IInstance instance, Set<String> expectedRingAddresses)
```
Assert ring contains exactly the expected instances.

### awaitRingJoin()
```java
List<RingInstanceDetails> awaitRingJoin(IInstance instance, IInstance expectedInRing)
List<RingInstanceDetails> awaitRingJoin(IInstance instance, String expectedInRing)
void awaitRingJoin(Cluster cluster, int[] nodes, IInvokableInstance expectedInRing)
```
Wait for target to join ring (status=Up, state=Normal).

### awaitRingHealthy()
```java
List<RingInstanceDetails> awaitRingHealthy(IInstance src)
```
Wait for all instances in ring to be Up and Normal.

### awaitRingStatus()
```java
List<RingInstanceDetails> awaitRingStatus(IInstance instance, IInstance expectedInRing, String status)
```
Wait for target to have specific status (Up/Down).

### awaitRingState()
```java
List<RingInstanceDetails> awaitRingState(IInstance instance, IInstance expectedInRing, String state)
```
Wait for target to have specific state.

### gossipInfo()
```java
Map<String, Map<String, String>> gossipInfo(IInstance inst)
```
Get gossip information from nodetool gossipinfo.

**Returns:** Map from address to state map (generation, heartbeat, STATUS, etc.)

### assertGossipInfo()
```java
void assertGossipInfo(IInstance instance, InetSocketAddress expectedInGossip,
                     int expectedGeneration, int expectedHeartbeat)
```
Assert specific gossip generation and heartbeat values.

### awaitGossipStatus()
```java
Map<String, Map<String, String>> awaitGossipStatus(IInstance instance,
                                                   IInstance expectedInGossip,
                                                   String targetStatus)
```
Wait for target to have specific gossip status (e.g., "NORMAL").

### awaitGossipSchemaMatch()
```java
void awaitGossipSchemaMatch(ICluster<? extends IInstance> cluster)
void awaitGossipSchemaMatch(IInstance instance)
```
Wait for schema IDs to match in gossip across cluster.

### awaitGossipStateMatch()
```java
void awaitGossipStateMatch(ICluster<? extends IInstance> cluster,
                          IInstance expectedInGossip,
                          ApplicationState key)
```
Wait for specific ApplicationState value to converge across cluster.

## System Tables

### awaitInPeers()
```java
void awaitInPeers(Cluster cluster, int[] nodes, IInstance expectedInPeers)
void awaitInPeers(IInstance instance, IInstance expectedInPeers)
```
Wait for target to appear in system.peers with full info (tokens, dc, rack).

### isInPeers()
```java
boolean isInPeers(IInstance instance, IInstance expectedInPeers)
```
Check if target is in system.peers with complete information.

## Directories & Files

### getDataDirectories()
```java
List<File> getDataDirectories(IInstance instance)
```
Get all data directories for instance.

### getCommitLogDirectory()
```java
File getCommitLogDirectory(IInstance instance)
```
Get commitlog directory.

### getHintsDirectory()
```java
File getHintsDirectory(IInstance instance)
```
Get hints directory.

### getSavedCachesDirectory()
```java
File getSavedCachesDirectory(IInstance instance)
```
Get saved caches directory.

### getJournalDirectory()
```java
File getJournalDirectory(IInstance instance)
```
Get Accord journal directory.

### getCdcRawDirectory()
```java
File getCdcRawDirectory(IInstance instance)
```
Get CDC raw directory.

### getDirectories()
```java
List<File> getDirectories(IInstance instance)
```
Get all writable directories (data, commitlog, hints, saved caches, journal, cdc).

### cleanup()
```java
void cleanup(IInvokableInstance inst)
```
Stop instance, delete all directories, restart with clean state.

## Node Operations

### decommission()
```java
boolean decommission(IInvokableInstance leaving)
```
Decommission node. Returns true if successful.

**Example:**
```java
boolean success = ClusterUtils.decommission(cluster.get(3));
Assert.assertTrue("Decommission failed", success);

ClusterUtils.assertNotInRing(cluster.get(1), cluster.get(3));
```

### getNodeId()
```java
NodeId getNodeId(IInvokableInstance target)
int getNodeId(IInvokableInstance target, IInvokableInstance executor)
```
Get NodeId for target instance.

### cancelInProgressSequences()
```java
boolean cancelInProgressSequences(IInvokableInstance executor)
boolean cancelInProgressSequences(NodeId nodeId, IInvokableInstance executor)
```
Cancel in-progress sequences for node. Returns true if successful.

### mode()
```java
StorageService.Mode mode(IInvokableInstance inst)
```
Get current StorageService operation mode.

**Modes:** STARTING, NORMAL, JOINING, LEAVING, DECOMMISSIONED, MOVING, DRAINING, DRAINED

### assertModeJoined()
```java
void assertModeJoined(IInvokableInstance inst)
```
Assert instance is in NORMAL mode.

### isMigrating()
```java
boolean isMigrating(IInvokableInstance instance)
```
Check if CMS is currently migrating.

## Log Monitoring

### runAndWaitForLogs()
```java
void runAndWaitForLogs(Runnable r, String waitString, AbstractCluster<I> cluster)
void runAndWaitForLogs(Runnable r, String waitString, IInstance... instances)
```
Mark log positions, run operation, wait for log message to appear on all instances.

**Example:**
```java
ClusterUtils.runAndWaitForLogs(
    () -> cluster.get(2).nodetool("bootstrap"),
    "Bootstrap completed",
    cluster.get(1), cluster.get(2), cluster.get(3)
);
```

## Address Management

### updateAddress()
```java
void updateAddress(IInstance instance, String address)
```
Change instance's address. Only call when instance is down.

**Warning:** Modifies broadcast_address, listen_address, broadcast_rpc_address, rpc_address.

### getBroadcastAddressHostWithPortString()
```java
String getBroadcastAddressHostWithPortString(IInstance target)
```
Get broadcast address in "host:port" format (e.g., "127.0.0.1:7190").

### getNativeInetSocketAddress()
```java
InetSocketAddress getNativeInetSocketAddress(IInstance target)
```
Get native protocol address (CQL port) as InetSocketAddress.

### getIntConfig()
```java
int getIntConfig(IInstanceConfig config, String configName, int defaultValue)
```
Safely get integer config value with fallback.

## Accord Testing

### queryTxnState()
```java
<T extends IInstance> LinkedHashMap<String, SimpleQueryResult> queryTxnState(
    AbstractCluster<T> cluster,
    TxnId txnId,
    int... nodes)
```
Query Accord transaction state from virtual table across nodes.

### queryTxnStateAsString()
```java
<T extends IInstance> String queryTxnStateAsString(AbstractCluster<T> cluster, TxnId txnId, int... nodes)
<T extends IInstance> void queryTxnStateAsString(StringBuilder sb, AbstractCluster<T> cluster, TxnId txnId, int... nodes)
```
Get Accord transaction state as formatted string.

### tableId()
```java
TableId tableId(Cluster cluster, String ks, String table)
```
Get TableId for keyspace and table.

### awaitAccordEpochReady()
```java
void awaitAccordEpochReady(Cluster cluster, long epoch)
```
Wait for Accord epoch to be ready for reads on all instances.

## Utility Classes

### Range
```java
public static class Range implements Serializable {
    public final String left, right;

    public Range(String left, String right)
    public Range(long left, long right)

    public long left()
    public long right()
}
```
Represents a token range.

### RingInstanceDetails
```java
public static final class RingInstanceDetails {
    private final String address;
    private final String rack;
    private final String status;  // Up/Down
    private final String state;   // Normal/Leaving/Joining/Moving
    private final String token;

    public String getAddress()
    public String getRack()
    public String getStatus()
    public String getState()
    public String getToken()
}
```
Parsed nodetool ring output for single instance.
