/**
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

import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.TokenRange;
import org.apache.cassandra.thrift.UnavailableException;


public interface StorageServiceMBean
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
     * Fetch a string representation of the token.
     *
     * @return a string token
     */
    public String getToken();

    /**
     * Fetch a string representation of the Cassandra version.
     * @return A string representation of the Cassandra version.
     */
    public String getReleaseVersion();

    /**
     * Get the list of all data file locations from conf
     * @return String array of all locations
     */
    public String[] getAllDataFileLocations();

    /**
     * Get the list of data file locations for a given table
     * @param table the table to get locations for.
     * @return String array of all locations
     */
    public String[] getAllDataFileLocationsForTable(String table);

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
    public Map<Range, List<String>> getRangeToEndpointMap(String keyspace);

    /**
     * Retrieve a map of range to rpc addresses that describe the ring topology
     * of a Cassandra cluster.
     *
     * @return mapping of ranges to rpc addresses
     */
    public Map<Range, List<String>> getRangeToRpcaddressMap(String keyspace);

    /**
     * The same as {@code describeRing(String)} but converts TokenRange to the String for JMX compatibility
     *
     * @param keyspace The keyspace to fetch information about
     *
     * @return a List of TokenRange(s) converted to String for the given keyspace
     *
     * @throws InvalidRequestException if there is no ring information available about keyspace
     */
    public List <String> describeRingJMX(String keyspace) throws InvalidRequestException;

    /**
     * Retrieve a map of pending ranges to endpoints that describe the ring topology
     * @param keyspace the keyspace to get the pending range map for.
     * @return a map of pending ranges to endpoints
     */
    public Map<Range, List<String>> getPendingRangeToEndpointMap(String keyspace);

    /**
     * Retrieve a map of tokens to endpoints, including the bootstrapping
     * ones.
     *
     * @return a map of tokens to endpoints
     */
    public Map<Token, String> getTokenToEndpointMap();

    /**
     * Numeric load value.
     */
    public double getLoad();

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
     * @param table keyspace name also known as table
     * @param cf Column family name
     * @param key - key for which we need to find the endpoint return value -
     * the endpoint responsible for this key
     */
    public List<InetAddress> getNaturalEndpoints(String table, String cf, String key);
    public List<InetAddress> getNaturalEndpoints(String table, ByteBuffer key);

    /**
     * Takes the snapshot for the given tables. A snapshot name must be specified.
     *
     * @param tag the tag given to the snapshot; may not be null or empty
     * @param tableNames the name of the tables to snapshot; empty means "all."
     */
    public void takeSnapshot(String tag, String... tableNames) throws IOException;

    /**
     * Remove the snapshot with the given name from the given tables.
     * If no tag is specified we will remove all snapshots.
     */
    public void clearSnapshot(String tag, String... tableNames) throws IOException;

    /**
     * Forces major compaction of a single keyspace
     */
    public void forceTableCompaction(String tableName, String... columnFamilies) throws IOException, ExecutionException, InterruptedException;

    /**
     * Trigger a cleanup of keys on a single keyspace
     */
    public void forceTableCleanup(String tableName, String... columnFamilies) throws IOException, ExecutionException, InterruptedException;

    /**
     * Scrub (deserialize + reserialize at the latest version, skipping bad rows if any) the given keyspace.
     * If columnFamilies array is empty, all CFs are scrubbed.
     *
     * Scrubbed CFs will be snapshotted first.
     */
    public void scrub(String tableName, String... columnFamilies) throws IOException, ExecutionException, InterruptedException;

    /**
     * Rewrite all sstables to the latest version.
     * Unlike scrub, it doesn't skip bad rows and do not snapshot sstables first.
     */
    public void upgradeSSTables(String tableName, String... columnFamilies) throws IOException, ExecutionException, InterruptedException;

    /**
     * Flush all memtables for the given column families, or all columnfamilies for the given table
     * if none are explicitly listed.
     * @param tableName
     * @param columnFamilies
     * @throws IOException
     */
    public void forceTableFlush(String tableName, String... columnFamilies) throws IOException, ExecutionException, InterruptedException;

    /**
     * Triggers proactive repair for given column families, or all columnfamilies for the given table
     * if none are explicitly listed.
     * @param tableName
     * @param columnFamilies
     * @throws IOException
     */
    public void forceTableRepair(String tableName, String... columnFamilies) throws IOException;

    /**
     * Triggers proactive repair but only for the node primary range.
     */
    public void forceTableRepairPrimaryRange(String tableName, String... columnFamilies) throws IOException;

    public void forceTerminateAllRepairSessions();

    /**
     * transfer this node's data to other machines and remove it from service.
     */
    public void decommission() throws InterruptedException;

    /**
     * @param newToken token to move this node to.
     * This node will unload its data onto its neighbors, and bootstrap to the new token.
     */
    public void move(String newToken) throws IOException, InterruptedException, ConfigurationException;

    /**
     * removeToken removes token (and all data associated with
     * enpoint that had it) from the ring
     */
    public void removeToken(String token);

    /**
     * Get the status of a token removal.
     */
    public String getRemovalStatus();

    /**
     * Force a remove operation to finish.
     */
    public void forceRemoveCompletion();

    /** set the logging level at runtime */
    public void setLog4jLevel(String classQualifier, String level);

    /** get the operational mode (leaving, joining, normal, decommissioned, client) **/
    public String getOperationMode();

    /** get the progress of a drain operation */
    public String getDrainProgress();

    /** makes node unavailable for writes, flushes memtables and replays commitlog. */
    public void drain() throws IOException, InterruptedException, ExecutionException;

    /**
     * Truncates (deletes) the given columnFamily from the provided keyspace.
     * Calling truncate results in actual deletion of all data in the cluster
     * under the given columnFamily and it will fail unless all hosts are up.
     * All data in the given column family will be deleted, but its definition
     * will not be affected.
     *
     * @param keyspace The keyspace to delete from
     * @param columnFamily The column family to delete data from.
     *
     * @throws UnavailableException if some of the hosts in the ring are down.
     */
    public void truncate(String keyspace, String columnFamily) throws UnavailableException, TimeoutException, IOException;

    /** force hint delivery to an endpoint **/
    public void deliverHints(String host) throws UnknownHostException;

    /** save row and key caches */
    public void saveCaches() throws ExecutionException, InterruptedException;

    /**
     * given a list of tokens (representing the nodes in the cluster), returns
     *   a mapping from "token -> %age of cluster owned by that token"
     */
    public Map<Token, Float> getOwnership();

    public List<String> getKeyspaces();

    /**
     * Change endpointsnitch class and dynamic-ness (and dynamic attributes) at runtime
     * @param epSnitchClassName        the canonical path name for a class implementing IEndpointSnitch
     * @param dynamic                  boolean that decides whether dynamicsnitch is used or not
     * @param dynamicUpdateInterval    integer, in ms (default 100)
     * @param dynamicResetInterval     integer, in ms (default 600,000)
     * @param dynamicBadnessThreshold  double, (default 0.0)
     * @throws ConfigurationException  classname not found on classpath
     */
    public void updateSnitch(String epSnitchClassName, Boolean dynamic, Integer dynamicUpdateInterval, Integer dynamicResetInterval, Double dynamicBadnessThreshold) throws ConfigurationException;

    // allows a user to forcibly 'kill' a sick node
    public void stopGossiping();

    // allows a user to recover a forcibly 'killed' node
    public void startGossiping();

    // to determine if gossip is disabled
    public boolean isInitialized();

    // allows a user to disable thrift
    public void stopRPCServer();

    // allows a user to reenable thrift
    public void startRPCServer();

    // to determine if thrift is running
    public boolean isRPCServerRunning();

    public void invalidateKeyCaches(String ks, String... cfs) throws IOException;
    public void invalidateRowCaches(String ks, String... cfs) throws IOException;

    // allows a node that have been started without joining the ring to join it
    public void joinRing() throws IOException, org.apache.cassandra.config.ConfigurationException;
    public boolean isJoined();

    public int getExceptionCount();

    public int getCompactionThroughputMbPerSec();
    public void setCompactionThroughputMbPerSec(int value);

    public void bulkLoad(String directory);

    public void rescheduleFailedDeletions();

    /**
     * Load new SSTables to the given keyspace/columnFamily
     *
     * @param ksName The parent keyspace name
     * @param cfName The ColumnFamily name where SSTables belong
     */
    public void loadNewSSTables(String ksName, String cfName);
}
