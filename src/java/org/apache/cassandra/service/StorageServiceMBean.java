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
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import javax.annotation.Nullable;
import javax.management.NotificationEmitter;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.TabularData;

import org.apache.cassandra.db.ColumnFamilyStoreMBean;
import org.apache.cassandra.exceptions.ConfigurationException;

public interface StorageServiceMBean extends NotificationEmitter
{
    /**
     * Retrieve the list of live nodes in the cluster, where "liveness" is
     * determined by the failure detector of the node being queried.
     *
     * @return set of IP addresses, as Strings
     */
    @Deprecated public List<String> getLiveNodes();
    public List<String> getLiveNodesWithPort();

    /**
     * Retrieve the list of unreachable nodes in the cluster, as determined
     * by this node's failure detector.
     *
     * @return set of IP addresses, as Strings
     */
    @Deprecated public List<String> getUnreachableNodes();
    public List<String> getUnreachableNodesWithPort();

    /**
     * Retrieve the list of nodes currently bootstrapping into the ring.
     *
     * @return set of IP addresses, as Strings
     */
    @Deprecated public List<String> getJoiningNodes();
    public List<String> getJoiningNodesWithPort();

    /**
     * Retrieve the list of nodes currently leaving the ring.
     *
     * @return set of IP addresses, as Strings
     */
    @Deprecated public List<String> getLeavingNodes();
    public List<String> getLeavingNodesWithPort();

    /**
     * Retrieve the list of nodes currently moving in the ring.
     *
     * @return set of IP addresses, as Strings
     */
    @Deprecated public List<String> getMovingNodes();
    public List<String> getMovingNodesWithPort();

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
     * Fetch the replication factor for a given keyspace.
     * @return An integer that represents replication factor for the given keyspace.
     */
    public String getKeyspaceReplicationInfo(String keyspaceName);

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
    @Deprecated public Map<List<String>, List<String>> getRangeToEndpointMap(String keyspace);
    public Map<List<String>, List<String>> getRangeToEndpointWithPortMap(String keyspace);

    /**
     * Retrieve a map of range to rpc addresses that describe the ring topology
     * of a Cassandra cluster.
     *
     * @return mapping of ranges to rpc addresses
     */
    @Deprecated public Map<List<String>, List<String>> getRangeToRpcaddressMap(String keyspace);
    public Map<List<String>, List<String>> getRangeToNativeaddressWithPortMap(String keyspace);

    /**
     * The same as {@code describeRing(String)} but converts TokenRange to the String for JMX compatibility
     *
     * @param keyspace The keyspace to fetch information about
     *
     * @return a List of TokenRange(s) converted to String for the given keyspace
     */
    @Deprecated public List <String> describeRingJMX(String keyspace) throws IOException;
    public List<String> describeRingWithPortJMX(String keyspace) throws IOException;

    /**
     * Retrieve a map of pending ranges to endpoints that describe the ring topology
     * @param keyspace the keyspace to get the pending range map for.
     * @return a map of pending ranges to endpoints
     */
    @Deprecated public Map<List<String>, List<String>> getPendingRangeToEndpointMap(String keyspace);
    public Map<List<String>, List<String>> getPendingRangeToEndpointWithPortMap(String keyspace);

    /**
     * Retrieve a map of tokens to endpoints, including the bootstrapping
     * ones.
     *
     * @return a map of tokens to endpoints in ascending order
     */
    @Deprecated public Map<String, String> getTokenToEndpointMap();
    public Map<String, String> getTokenToEndpointWithPortMap();

    /** Retrieve this hosts unique ID */
    public String getLocalHostId();

    /** {@link StorageServiceMBean#getEndpointToHostId} */
    @Deprecated
    public Map<String, String> getHostIdMap();

    /** Retrieve the mapping of endpoint to host ID */
    @Deprecated public Map<String, String> getEndpointToHostId();
    public Map<String, String> getEndpointWithPortToHostId();

    /** Retrieve the mapping of host ID to endpoint */
    @Deprecated public Map<String, String> getHostIdToEndpoint();
    public Map<String, String> getHostIdToEndpointWithPort();

    /** Human-readable load value */
    public String getLoadString();

    /** Human-readable load value.  Keys are IP addresses. */
    @Deprecated public Map<String, String> getLoadMap();
    public Map<String, String> getLoadMapWithPort();

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
    @Deprecated public List<InetAddress> getNaturalEndpoints(String keyspaceName, String cf, String key);
    public List<String> getNaturalEndpointsWithPort(String keyspaceName, String cf, String key);
    @Deprecated public List<InetAddress> getNaturalEndpoints(String keyspaceName, ByteBuffer key);
    public List<String> getNaturalEndpointsWithPort(String keysapceName, ByteBuffer key);

    public List<String> getReplicas(String keyspaceName, String cf, String key);

    /**
     * @deprecated use {@link #takeSnapshot(String tag, Map options, String... entities)} instead.
     */
    @Deprecated
    public void takeSnapshot(String tag, String... keyspaceNames) throws IOException;

    /**
     * @deprecated use {@link #takeSnapshot(String tag, Map options, String... entities)} instead.
     */
    @Deprecated
    public void takeTableSnapshot(String keyspaceName, String tableName, String tag) throws IOException;

    /**
     * @deprecated use {@link #takeSnapshot(String tag, Map options, String... entities)} instead.
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
     * Removes extraneous entries in system.size_estimates.
     */
    public void cleanupSizeEstimates();

    /**
     * Forces major compaction of a single keyspace
     */
    public void forceKeyspaceCompaction(boolean splitOutput, String keyspaceName, String... tableNames) throws IOException, ExecutionException, InterruptedException;

    @Deprecated
    public int relocateSSTables(String keyspace, String ... cfnames) throws IOException, ExecutionException, InterruptedException;
    public int relocateSSTables(int jobs, String keyspace, String ... cfnames) throws IOException, ExecutionException, InterruptedException;
    /**
     * Forces major compaction of specified token range in a single keyspace
     */
    public void forceKeyspaceCompactionForTokenRange(String keyspaceName, String startToken, String endToken, String... tableNames) throws IOException, ExecutionException, InterruptedException;

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
    @Deprecated
    public int scrub(boolean disableSnapshot, boolean skipCorrupted, boolean checkData, int jobs, String keyspaceName, String... columnFamilies) throws IOException, ExecutionException, InterruptedException;

    public int scrub(boolean disableSnapshot, boolean skipCorrupted, boolean checkData, boolean reinsertOverflowedTTL, int jobs, String keyspaceName, String... columnFamilies) throws IOException, ExecutionException, InterruptedException;

    /**
     * Verify (checksums of) the given keyspace.
     * If tableNames array is empty, all CFs are verified.
     *
     * The entire sstable will be read to ensure each cell validates if extendedVerify is true
     */
    public int verify(boolean extendedVerify, String keyspaceName, String... tableNames) throws IOException, ExecutionException, InterruptedException;
    public int verify(boolean extendedVerify, boolean checkVersion, boolean diskFailurePolicy, boolean mutateRepairStatus, boolean checkOwnsTokens, boolean quick, String keyspaceName, String... tableNames) throws IOException, ExecutionException, InterruptedException;

    /**
     * Rewrite all sstables to the latest version.
     * Unlike scrub, it doesn't skip bad rows and do not snapshot sstables first.
     */
    @Deprecated
    public int upgradeSSTables(String keyspaceName, boolean excludeCurrentVersion, String... tableNames) throws IOException, ExecutionException, InterruptedException;
    public int upgradeSSTables(String keyspaceName, boolean excludeCurrentVersion, int jobs, String... tableNames) throws IOException, ExecutionException, InterruptedException;

    /**
     * Rewrites all sstables from the given tables to remove deleted data.
     * The tombstone option defines the granularity of the procedure: ROW removes deleted partitions and rows, CELL also removes overwritten or deleted cells.
     */
    public int garbageCollect(String tombstoneOption, int jobs, String keyspaceName, String... tableNames) throws IOException, ExecutionException, InterruptedException;

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

    public void forceTerminateAllRepairSessions();

    public void setRepairSessionMaxTreeDepth(int depth);

    public int getRepairSessionMaxTreeDepth();

    /**
     * Get the status of a given parent repair session.
     * @param cmd the int reference returned when issuing the repair
     * @return status of parent repair from enum {@link org.apache.cassandra.repair.RepairRunnable.Status}
     * followed by final message or messages of the session
     */
    @Nullable
    public List<String> getParentRepairStatus(int cmd);

    /**
     * transfer this node's data to other machines and remove it from service.
     * @param force Decommission even if this will reduce N to be less than RF.
     */
    public void decommission(boolean force) throws InterruptedException;

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
    @Deprecated public String getRemovalStatus();
    public String getRemovalStatusWithPort();

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
    @Deprecated public Map<InetAddress, Float> getOwnership();
    public Map<String, Float> getOwnershipWithPort();

    /**
     * Effective ownership is % of the data each node owns given the keyspace
     * we calculate the percentage using replication factor.
     * If Keyspace == null, this method will try to verify if all the keyspaces
     * in the cluster have the same replication strategies and if yes then we will
     * use the first else a empty Map is returned.
     */
    @Deprecated public Map<InetAddress, Float> effectiveOwnership(String keyspace) throws IllegalStateException;
    public Map<String, Float> effectiveOwnershipWithPort(String keyspace) throws IllegalStateException;

    public List<String> getKeyspaces();

    public List<String> getNonSystemKeyspaces();

    public List<String> getNonLocalStrategyKeyspaces();

    @Deprecated public Map<String, String> getViewBuildStatuses(String keyspace, String view);
    public Map<String, String> getViewBuildStatusesWithPort(String keyspace, String view);

    /**
     * Change endpointsnitch class and dynamic-ness (and dynamic attributes) at runtime.
     *
     * This method is used to change the snitch implementation and/or dynamic snitch parameters.
     * If {@code epSnitchClassName} is specified, it will configure a new snitch instance and make it a
     * 'dynamic snitch' if {@code dynamic} is specified and {@code true}.
     *
     * The parameters {@code dynamicUpdateInterval}, {@code dynamicResetInterval} and {@code dynamicBadnessThreshold}
     * can be specified individually to update the parameters of the dynamic snitch during runtime.
     *
     * @param epSnitchClassName        the canonical path name for a class implementing IEndpointSnitch
     * @param dynamic                  boolean that decides whether dynamicsnitch is used or not - only valid, if {@code epSnitchClassName} is specified
     * @param dynamicUpdateInterval    integer, in ms (defaults to the value configured in cassandra.yaml, which defaults to 100)
     * @param dynamicResetInterval     integer, in ms (defaults to the value configured in cassandra.yaml, which defaults to 600,000)
     * @param dynamicBadnessThreshold  double, (defaults to the value configured in cassandra.yaml, which defaults to 0.0)
     */
    public void updateSnitch(String epSnitchClassName, Boolean dynamic, Integer dynamicUpdateInterval, Integer dynamicResetInterval, Double dynamicBadnessThreshold) throws ClassNotFoundException;

    /*
      Update dynamic_snitch_update_interval_in_ms
     */
    public void setDynamicUpdateInterval(int dynamicUpdateInterval);

    /*
      Get dynamic_snitch_update_interval_in_ms
     */
    public int getDynamicUpdateInterval();

    // allows a user to forcibly 'kill' a sick node
    public void stopGossiping();

    // allows a user to recover a forcibly 'killed' node
    public void startGossiping();

    // allows a user to see whether gossip is running or not
    public boolean isGossipRunning();

    // allows a user to forcibly completely stop cassandra
    public void stopDaemon();

    // to determine if initialization has completed
    public boolean isInitialized();

    public void stopNativeTransport();
    public void startNativeTransport();
    public boolean isNativeTransportRunning();
    public void enableNativeTransportOldProtocolVersions();
    public void disableNativeTransportOldProtocolVersions();

    // allows a node that have been started without joining the ring to join it
    public void joinRing() throws IOException;
    public boolean isJoined();
    public boolean isDrained();
    public boolean isDraining();
    /** Check if currently bootstrapping.
     * Note this becomes false before {@link org.apache.cassandra.db.SystemKeyspace#bootstrapComplete()} is called,
     * as setting bootstrap to complete is called only when the node joins the ring.
     * @return True prior to bootstrap streaming completing. False prior to start of bootstrap and post streaming.
     */
    public boolean isBootstrapMode();

    public void setRpcTimeout(long value);
    public long getRpcTimeout();

    public void setReadRpcTimeout(long value);
    public long getReadRpcTimeout();

    public void setRangeRpcTimeout(long value);
    public long getRangeRpcTimeout();

    public void setWriteRpcTimeout(long value);
    public long getWriteRpcTimeout();

    public void setInternodeTcpConnectTimeoutInMS(int value);
    public int getInternodeTcpConnectTimeoutInMS();

    public void setInternodeTcpUserTimeoutInMS(int value);
    public int getInternodeTcpUserTimeoutInMS();

    public void setCounterWriteRpcTimeout(long value);
    public long getCounterWriteRpcTimeout();

    public void setCasContentionTimeout(long value);
    public long getCasContentionTimeout();

    public void setTruncateRpcTimeout(long value);
    public long getTruncateRpcTimeout();

    public void setStreamThroughputMbPerSec(int value);
    public int getStreamThroughputMbPerSec();

    public void setInterDCStreamThroughputMbPerSec(int value);
    public int getInterDCStreamThroughputMbPerSec();

    public int getCompactionThroughputMbPerSec();
    public void setCompactionThroughputMbPerSec(int value);

    public int getBatchlogReplayThrottleInKB();
    public void setBatchlogReplayThrottleInKB(int value);

    public int getConcurrentCompactors();
    public void setConcurrentCompactors(int value);

    public int getConcurrentValidators();
    public void setConcurrentValidators(int value);

    public int getSSTablePreemptiveOpenIntervalInMB();
    public void setSSTablePreemptiveOpenIntervalInMB(int intervalInMB);

    public boolean getMigrateKeycacheOnCompaction();
    public void setMigrateKeycacheOnCompaction(boolean invalidateKeyCacheOnCompaction);

    public int getConcurrentViewBuilders();
    public void setConcurrentViewBuilders(int value);

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

    /**
     * Same as {@link #rebuild(String)}, but only for specified keyspace and ranges.
     *
     * @param sourceDc Name of DC from which to select sources for streaming or null to pick any node
     * @param keyspace Name of the keyspace which to rebuild or null to rebuild all keyspaces.
     * @param tokens Range of tokens to rebuild or null to rebuild all token ranges. In the format of:
     *               "(start_token_1,end_token_1],(start_token_2,end_token_2],...(start_token_n,end_token_n]"
     */
    public void rebuild(String sourceDc, String keyspace, String tokens, String specificSources);

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
     *
     * @see ColumnFamilyStoreMBean#loadNewSSTables()
     */
    @Deprecated
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

    public void reloadLocalSchema();

    /**
     * Enables/Disables tracing for the whole system.
     *
     * @param probability
     *            ]0,1[ will enable tracing on a partial number of requests with the provided probability. 0 will
     *            disable tracing and 1 will enable tracing for all requests (which mich severely cripple the system)
     */
    public void setTraceProbability(double probability);

    public Map<String, List<CompositeData>> samplePartitions(int duration, int capacity, int count, List<String> samplers) throws OpenDataException;

    /**
     * Returns the configured tracing probability.
     */
    public double getTraceProbability();

    void disableAutoCompaction(String ks, String ... tables) throws IOException;
    void enableAutoCompaction(String ks, String ... tables) throws IOException;
    Map<String, Boolean> getAutoCompactionStatus(String ks, String... tables) throws IOException;

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

    /** Returns the threshold for warning queries due to a large batch size */
    public int getBatchSizeWarnThreshold();
    /** Sets the threshold for warning queries due to a large batch size */
    public void setBatchSizeWarnThreshold(int batchSizeDebugThreshold);

    /** Sets the hinted handoff throttle in kb per second, per delivery thread. */
    public void setHintedHandoffThrottleInKB(int throttleInKB);

    /**
     * Resume bootstrap streaming when there is failed data streaming.
     *
     *
     * @return true if the node successfully starts resuming. (this does not mean bootstrap streaming was success.)
     */
    public boolean resumeBootstrap();


    /** Clears the history of clients that have connected in the past **/
    void clearConnectionHistory();
    public void disableAuditLog();
    public void enableAuditLog(String loggerName, String includedKeyspaces, String excludedKeyspaces, String includedCategories, String excludedCategories, String includedUsers, String excludedUsers) throws ConfigurationException;
    public boolean isAuditLogEnabled();
    public String getCorruptedTombstoneStrategy();
    public void setCorruptedTombstoneStrategy(String strategy);
}
