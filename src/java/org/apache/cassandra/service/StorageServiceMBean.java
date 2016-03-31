/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.service;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import javax.management.NotificationEmitter;
import javax.management.openmbean.TabularData;

public interface StorageServiceMBean extends NotificationEmitter
{
    /**
     * Retrieve the list of live nodes in the cluster, where "liveness" is
     * determined by the failure detector of the node being queried.
     *
     * @return set of IP addresses, as Strings
     */
    public List<String> getLiveNodes();

    /**
     * Retrieve the list of unreachable nodes in the cluster, as determined
     * by this node's failure detector.
     *
     * @return set of IP addresses, as Strings
     */
    public List<String> getUnreachableNodes();

    /**
     * Retrieve the list of nodes currently bootstrapping into the ring.
     *
     * @return set of IP addresses, as Strings
     */
    public List<String> getJoiningNodes();

    /**
     * Retrieve the list of nodes currently leaving the ring.
     *
     * @return set of IP addresses, as Strings
     */
    public List<String> getLeavingNodes();

    /**
     * Retrieve the list of nodes currently moving in the ring.
     *
     * @return set of IP addresses, as Strings
     */
    public List<String> getMovingNodes();

    /**
     * Fetch string representations of the tokens for this node.
     *
     * @return a collection of tokens formatted as strings
     */
    public List<String> getTokens();

    /**
     * Fetch string representations of the tokens for a specified node.
     *
     * @param endpoint string representation of an node
     * @return a collection of tokens formatted as strings
     */
    public List<String> getTokens(String endpoint) throws UnknownHostException;

    /**
     * Fetch a string representation of the Cassandra version.
     * @return A string representation of the Cassandra version.
     */
    public String getReleaseVersion();

    /**
     * Fetch a string representation of the current Schema version.
     * @return A string representation of the Schema version.
     */
    public String getSchemaVersion();


    /**
     * Get the list of all data file locations from conf
     * @return String array of all locations
     */
    public String[] getAllDataFileLocations();

    /**
     * Get location of the commit log
     * @return a string path
     */
    public String getCommitLogLocation();

    /**
     * Get location of the saved caches dir
     * @return a string path
     */
    public String getSavedCachesLocation();

    /**
     * Retrieve a map of range to end points that describe the ring topology
     * of a Cassandra cluster.
     *
     * @return mapping of ranges to end points
     */
    public Map<List<String>, List<String>> getRangeToEndpointMap(String keyspace);

    /**
     * Retrieve a map of range to rpc addresses that describe the ring topology
     * of a Cassandra cluster.
     *
     * @return mapping of ranges to rpc addresses
     */
    public Map<List<String>, List<String>> getRangeToRpcaddressMap(String keyspace);

    /**
     * The same as {@code describeRing(String)} but converts TokenRange to the String for JMX compatibility
     *
     * @param keyspace The keyspace to fetch information about
     *
     * @return a List of TokenRange(s) converted to String for the given keyspace
     */
    public List <String> describeRingJMX(String keyspace) throws IOException;

    /**
     * Retrieve a map of pending ranges to endpoints that describe the ring topology
     * @param keyspace the keyspace to get the pending range map for.
     * @return a map of pending ranges to endpoints
     */
    public Map<List<String>, List<String>> getPendingRangeToEndpointMap(String keyspace);

    /**
     * Retrieve a map of tokens to endpoints, including the bootstrapping
     * ones.
     *
     * @return a map of tokens to endpoints in ascending order
     */
    public Map<String, String> getTokenToEndpointMap();

    /** Retrieve this hosts unique ID */
    public String getLocalHostId();

    /** {@link StorageServiceMBean#getEndpointToHostId} */
    @Deprecated
    public Map<String, String> getHostIdMap();

    /** Retrieve the mapping of endpoint to host ID */
    public Map<String, String> getEndpointToHostId();

    /** Retrieve the mapping of host ID to endpoint */
    public Map<String, String> getHostIdToEndpoint();

    /** Human-readable load value */
    public String getLoadString();

    /** Human-readable load value.  Keys are IP addresses. */
    public Map<String, String> getLoadMap();

    /**
     * Return the generation value for this node.
     *
     * @return generation number
     */
    public int getCurrentGenerationNumber();

    /**
     * This method returns the N endpoints that are responsible for storing the
     * specified key i.e for replication.
     *
     * @param keyspaceName keyspace name
     * @param cf Column family name
     * @param key - key for which we need to find the endpoint return value -
     * the endpoint responsible for this key
     */
    public List<InetAddress> getNaturalEndpoints(String keyspaceName, String cf, String key);
    public List<InetAddress> getNaturalEndpoints(String keyspaceName, ByteBuffer key);

    /**
     * @deprecated use {@link #takeSnapshot(String tag, Map options, boolean keyspaces, String... entities)} instead.
     */
    @Deprecated
    public void takeSnapshot(String tag, String... keyspaceNames) throws IOException;

    /**
     * @deprecated use {@link #takeSnapshot(String tag, Map options, boolean keyspaces, String... entities)} instead.
     */
    @Deprecated
    public void takeTableSnapshot(String keyspaceName, String tableName, String tag) throws IOException;

    /**
     * @deprecated use {@link #takeSnapshot(String tag, Map options, boolean keyspaces, String... entities)} instead.
     */
    @Deprecated
    public void takeMultipleTableSnapshot(String tag, String... tableList) throws IOException;

    /**
     * Takes the snapshot of a multiple column family from different keyspaces. A snapshot name must be specified.
     * 
     * @param tag
     *            the tag given to the snapshot; may not be null or empty
     * @param options
     *            Map of options (skipFlush is the only supported option for now)
     * @param entities
     *            list of keyspaces / tables in the form of empty | ks1 ks2 ... | ks1.cf1,ks2.cf2,...
     */
    public void takeSnapshot(String tag, Map<String, String> options, String... entities) throws IOException;

    /**
     * Remove the snapshot with the given name from the given keyspaces.
     * If no tag is specified we will remove all snapshots.
     */
    public void clearSnapshot(String tag, String... keyspaceNames) throws IOException;

    /**
     *  Get the details of all the snapshot
     * @return A map of snapshotName to all its details in Tabular form.
     */
    public Map<String, TabularData> getSnapshotDetails();

    /**
     * Get the true size taken by all snapshots across all keyspaces.
     * @return True size taken by all the snapshots.
     */
    public long trueSnapshotsSize();

    /**
     * Forces refresh of values stored in system.size_estimates of all column families.
     */
    public void refreshSizeEstimates() throws ExecutionException;

    /**
     * Forces major compaction of a single keyspace
     */
    public void forceKeyspaceCompaction(boolean splitOutput, String keyspaceName, String... tableNames) throws IOException, ExecutionException, InterruptedException;

    @Deprecated
    public int relocateSSTables(String keyspace, String ... cfnames) throws IOException, ExecutionException, InterruptedException;
    public int relocateSSTables(int jobs, String keyspace, String ... cfnames) throws IOException, ExecutionException, InterruptedException;
    /**
     * Trigger a cleanup of keys on a single keyspace
     */
    @Deprecated
    public int forceKeyspaceCleanup(String keyspaceName, String... tables) throws IOException, ExecutionException, InterruptedException;
    public int forceKeyspaceCleanup(int jobs, String keyspaceName, String... tables) throws IOException, ExecutionException, InterruptedException;

    /**
     * Scrub (deserialize + reserialize at the latest version, skipping bad rows if any) the given keyspace.
     * If tableNames array is empty, all CFs are scrubbed.
     *
     * Scrubbed CFs will be snapshotted first, if disableSnapshot is false
     */
    @Deprecated
    public int scrub(boolean disableSnapshot, boolean skipCorrupted, String keyspaceName, String... tableNames) throws IOException, ExecutionException, InterruptedException;
    @Deprecated
    public int scrub(boolean disableSnapshot, boolean skipCorrupted, boolean checkData, String keyspaceName, String... tableNames) throws IOException, ExecutionException, InterruptedException;
    public int scrub(boolean disableSnapshot, boolean skipCorrupted, boolean checkData, int jobs, String keyspaceName, String... columnFamilies) throws IOException, ExecutionException, InterruptedException;

    /**
     * Verify (checksums of) the given keyspace.
     * If tableNames array is empty, all CFs are verified.
     *
     * The entire sstable will be read to ensure each cell validates if extendedVerify is true
     */
    public int verify(boolean extendedVerify, String keyspaceName, String... tableNames) throws IOException, ExecutionException, InterruptedException;

    /**
     * Rewrite all sstables to the latest version.
     * Unlike scrub, it doesn't skip bad rows and do not snapshot sstables first.
     */
    @Deprecated
    public int upgradeSSTables(String keyspaceName, boolean excludeCurrentVersion, String... tableNames) throws IOException, ExecutionException, InterruptedException;
    public int upgradeSSTables(String keyspaceName, boolean excludeCurrentVersion, int jobs, String... tableNames) throws IOException, ExecutionException, InterruptedException;

    /**
     * Flush all memtables for the given column families, or all columnfamilies for the given keyspace
     * if none are explicitly listed.
     * @param keyspaceName
     * @param tableNames
     * @throws IOException
     */
    public void forceKeyspaceFlush(String keyspaceName, String... tableNames) throws IOException, ExecutionException, InterruptedException;

    /**
     * Invoke repair asynchronously.
     * You can track repair progress by subscribing JMX notification sent from this StorageServiceMBean.
     * Notification format is:
     *   type: "repair"
     *   userObject: int array of length 2, [0]=command number, [1]=ordinal of ActiveRepairService.Status
     *
     * @param keyspace Keyspace name to repair. Should not be null.
     * @param options repair option.
     * @return Repair command number, or 0 if nothing to repair
     */
    public int repairAsync(String keyspace, Map<String, String> options);

    /**
     * @deprecated use {@link #repairAsync(String keyspace, Map options)} instead.
     */
    @Deprecated
    public int forceRepairAsync(String keyspace, boolean isSequential, Collection<String> dataCenters, Collection<String> hosts,  boolean primaryRange, boolean fullRepair, String... tableNames) throws IOException;

    /**
     * Invoke repair asynchronously.
     * You can track repair progress by subscribing JMX notification sent from this StorageServiceMBean.
     * Notification format is:
     *   type: "repair"
     *   userObject: int array of length 2, [0]=command number, [1]=ordinal of ActiveRepairService.Status
     *
     * @deprecated use {@link #repairAsync(String keyspace, Map options)} instead.
     *
     * @param parallelismDegree 0: sequential, 1: parallel, 2: DC parallel
     * @return Repair command number, or 0 if nothing to repair
     */
    @Deprecated
    public int forceRepairAsync(String keyspace, int parallelismDegree, Collection<String> dataCenters, Collection<String> hosts, boolean primaryRange, boolean fullRepair, String... tableNames);

    /**
     * @deprecated use {@link #repairAsync(String keyspace, Map options)} instead.
     */
    @Deprecated
    public int forceRepairRangeAsync(String beginToken, String endToken, String keyspaceName, boolean isSequential, Collection<String> dataCenters, Collection<String> hosts, boolean fullRepair, String... tableNames) throws IOException;

    /**
     * Same as forceRepairAsync, but handles a specified range
     *
     * @deprecated use {@link #repairAsync(String keyspace, Map options)} instead.
     *
     * @param parallelismDegree 0: sequential, 1: parallel, 2: DC parallel
     */
    @Deprecated
    public int forceRepairRangeAsync(String beginToken, String endToken, String keyspaceName, int parallelismDegree, Collection<String> dataCenters, Collection<String> hosts, boolean fullRepair, String... tableNames);

    /**
     * @deprecated use {@link #repairAsync(String keyspace, Map options)} instead.
     */
    @Deprecated
    public int forceRepairAsync(String keyspace, boolean isSequential, boolean isLocal, boolean primaryRange, boolean fullRepair, String... tableNames);

    /**
     * @deprecated use {@link #repairAsync(String keyspace, Map options)} instead.
     */
    @Deprecated
    public int forceRepairRangeAsync(String beginToken, String endToken, String keyspaceName, boolean isSequential, boolean isLocal, boolean fullRepair, String... tableNames);

    public void forceTerminateAllRepairSessions();

    /**
     * transfer this node's data to other machines and remove it from service.
     */
    public void decommission() throws InterruptedException;

    /**
     * @param newToken token to move this node to.
     * This node will unload its data onto its neighbors, and bootstrap to the new token.
     */
    public void move(String newToken) throws IOException;

    /**
     * removeToken removes token (and all data associated with
     * enpoint that had it) from the ring
     */
    public void removeNode(String token);

    /**
     * Get the status of a token removal.
     */
    public String getRemovalStatus();

    /**
     * Force a remove operation to finish.
     */
    public void forceRemoveCompletion();

    /**
     * set the logging level at runtime<br>
     * <br>
     * If both classQualifer and level are empty/null, it will reload the configuration to reset.<br>
     * If classQualifer is not empty but level is empty/null, it will set the level to null for the defined classQualifer<br>
     * If level cannot be parsed, then the level will be defaulted to DEBUG<br>
     * <br>
     * The logback configuration should have {@code < jmxConfigurator />} set
     * 
     * @param classQualifier The logger's classQualifer
     * @param level The log level
     * @throws Exception 
     * 
     *  @see ch.qos.logback.classic.Level#toLevel(String)
     */
    public void setLoggingLevel(String classQualifier, String level) throws Exception;

    /** get the runtime logging levels */
    public Map<String,String> getLoggingLevels();

    /** get the operational mode (leaving, joining, normal, decommissioned, client) **/
    public String getOperationMode();

    /** Returns whether the storage service is starting or not */
    public boolean isStarting();

    /** get the progress of a drain operation */
    public String getDrainProgress();

    /** makes node unavailable for writes, flushes memtables and replays commitlog. */
    public void drain() throws IOException, InterruptedException, ExecutionException;

    /**
     * Truncates (deletes) the given table from the provided keyspace.
     * Calling truncate results in actual deletion of all data in the cluster
     * under the given table and it will fail unless all hosts are up.
     * All data in the given column family will be deleted, but its definition
     * will not be affected.
     *
     * @param keyspace The keyspace to delete from
     * @param table The column family to delete data from.
     */
    public void truncate(String keyspace, String table)throws TimeoutException, IOException;

    /**
     * given a list of tokens (representing the nodes in the cluster), returns
     *   a mapping from {@code "token -> %age of cluster owned by that token"}
     */
    public Map<InetAddress, Float> getOwnership();

    /**
     * Effective ownership is % of the data each node owns given the keyspace
     * we calculate the percentage using replication factor.
     * If Keyspace == null, this method will try to verify if all the keyspaces
     * in the cluster have the same replication strategies and if yes then we will
     * use the first else a empty Map is returned.
     */
    public Map<InetAddress, Float> effectiveOwnership(String keyspace) throws IllegalStateException;

    public List<String> getKeyspaces();

    public List<String> getNonSystemKeyspaces();

    public Map<String, String> getViewBuildStatuses(String keyspace, String view);

    /**
     * Change endpointsnitch class and dynamic-ness (and dynamic attributes) at runtime
     * @param epSnitchClassName        the canonical path name for a class implementing IEndpointSnitch
     * @param dynamic                  boolean that decides whether dynamicsnitch is used or not
     * @param dynamicUpdateInterval    integer, in ms (default 100)
     * @param dynamicResetInterval     integer, in ms (default 600,000)
     * @param dynamicBadnessThreshold  double, (default 0.0)
     */
    public void updateSnitch(String epSnitchClassName, Boolean dynamic, Integer dynamicUpdateInterval, Integer dynamicResetInterval, Double dynamicBadnessThreshold) throws ClassNotFoundException;

    // allows a user to forcibly 'kill' a sick node
    public void stopGossiping();

    // allows a user to recover a forcibly 'killed' node
    public void startGossiping();

    // allows a user to see whether gossip is running or not
    public boolean isGossipRunning();

    // allows a user to forcibly completely stop cassandra
    public void stopDaemon();

    // to determine if gossip is disabled
    public boolean isInitialized();

    // allows a user to disable thrift
    public void stopRPCServer();

    // allows a user to reenable thrift
    public void startRPCServer();

    // to determine if thrift is running
    public boolean isRPCServerRunning();

    public void stopNativeTransport();
    public void startNativeTransport();
    public boolean isNativeTransportRunning();

    // allows a node that have been started without joining the ring to join it
    public void joinRing() throws IOException;
    public boolean isJoined();

    public void setRpcTimeout(long value);
    public long getRpcTimeout();

    public void setReadRpcTimeout(long value);
    public long getReadRpcTimeout();

    public void setRangeRpcTimeout(long value);
    public long getRangeRpcTimeout();

    public void setWriteRpcTimeout(long value);
    public long getWriteRpcTimeout();

    public void setCounterWriteRpcTimeout(long value);
    public long getCounterWriteRpcTimeout();

    public void setCasContentionTimeout(long value);
    public long getCasContentionTimeout();

    public void setTruncateRpcTimeout(long value);
    public long getTruncateRpcTimeout();

    public void setStreamingSocketTimeout(int value);
    public int getStreamingSocketTimeout();

    public void setStreamThroughputMbPerSec(int value);
    public int getStreamThroughputMbPerSec();

    public void setInterDCStreamThroughputMbPerSec(int value);
    public int getInterDCStreamThroughputMbPerSec();

    public int getCompactionThroughputMbPerSec();
    public void setCompactionThroughputMbPerSec(int value);

    public boolean isIncrementalBackupsEnabled();
    public void setIncrementalBackupsEnabled(boolean value);

    /**
     * Initiate a process of streaming data for which we are responsible from other nodes. It is similar to bootstrap
     * except meant to be used on a node which is already in the cluster (typically containing no data) as an
     * alternative to running repair.
     *
     * @param sourceDc Name of DC from which to select sources for streaming or null to pick any node
     */
    public void rebuild(String sourceDc);

    /** Starts a bulk load and blocks until it completes. */
    public void bulkLoad(String directory);

    /**
     * Starts a bulk load asynchronously and returns the String representation of the planID for the new
     * streaming session.
     */
    public String bulkLoadAsync(String directory);

    public void rescheduleFailedDeletions();

    /**
     * Load new SSTables to the given keyspace/table
     *
     * @param ksName The parent keyspace name
     * @param tableName The ColumnFamily name where SSTables belong
     */
    public void loadNewSSTables(String ksName, String tableName);

    /**
     * Return a List of Tokens representing a sample of keys across all ColumnFamilyStores.
     *
     * Note: this should be left as an operation, not an attribute (methods starting with "get")
     * to avoid sending potentially multiple MB of data when accessing this mbean by default.  See CASSANDRA-4452.
     *
     * @return set of Tokens as Strings
     */
    public List<String> sampleKeyRange();

    /**
     * rebuild the specified indexes
     */
    public void rebuildSecondaryIndex(String ksName, String cfName, String... idxNames);

    public void resetLocalSchema() throws IOException;

    /**
     * Enables/Disables tracing for the whole system. Only thrift requests can start tracing currently.
     *
     * @param probability
     *            ]0,1[ will enable tracing on a partial number of requests with the provided probability. 0 will
     *            disable tracing and 1 will enable tracing for all requests (which mich severely cripple the system)
     */
    public void setTraceProbability(double probability);

    /**
     * Returns the configured tracing probability.
     */
    public double getTraceProbability();

    void disableAutoCompaction(String ks, String ... tables) throws IOException;
    void enableAutoCompaction(String ks, String ... tables) throws IOException;

    public void deliverHints(String host) throws UnknownHostException;

    /** Returns the name of the cluster */
    public String getClusterName();
    /** Returns the cluster partitioner */
    public String getPartitionerName();

    /** Returns the threshold for warning of queries with many tombstones */
    public int getTombstoneWarnThreshold();
    /** Sets the threshold for warning queries with many tombstones */
    public void setTombstoneWarnThreshold(int tombstoneDebugThreshold);

    /** Returns the threshold for abandoning queries with many tombstones */
    public int getTombstoneFailureThreshold();
    /** Sets the threshold for abandoning queries with many tombstones */
    public void setTombstoneFailureThreshold(int tombstoneDebugThreshold);

    /** Returns the threshold for rejecting queries due to a large batch size */
    public int getBatchSizeFailureThreshold();
    /** Sets the threshold for rejecting queries due to a large batch size */
    public void setBatchSizeFailureThreshold(int batchSizeDebugThreshold);

    /** Sets the hinted handoff throttle in kb per second, per delivery thread. */
    public void setHintedHandoffThrottleInKB(int throttleInKB);

    /**
     * Resume bootstrap streaming when there is failed data streaming.
     *
     *
     * @return true if the node successfully starts resuming. (this does not mean bootstrap streaming was success.)
     */
    public boolean resumeBootstrap();
}
