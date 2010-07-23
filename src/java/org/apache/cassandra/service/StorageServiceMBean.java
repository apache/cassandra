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
import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.thrift.UnavailableException;

import java.net.InetAddress;


public interface StorageServiceMBean
{    
    /**
     * Retrieve the list of live nodes in the cluster, where "liveness" is
     * determined by the failure detector of the node being queried.
     *
     * @return set of IP addresses, as Strings
     */
    public Set<String> getLiveNodes();

    /**
     * Retrieve the list of unreachable nodes in the cluster, as determined
     * by this node's failure detector.
     *
     * @return set of IP addresses, as Strings
     */
    public Set<String> getUnreachableNodes();

    /**
     * Retrieve the list of nodes currently bootstrapping into the ring.
     *
     * @return set of IP addresses, as Strings
     */
    public Set<String> getJoiningNodes();

    /**
     * Retrieve the list of nodes currently leaving the ring.
     *
     * @return set of IP addresses, as Strings
     */
    public Set<String> getLeavingNodes();

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
     * Retrieve a map of range to end points that describe the ring topology
     * of a Cassandra cluster.
     *
     * @return mapping of ranges to end points
     */
    public Map<Range, List<String>> getRangeToEndpointMap(String keyspace);

    /**
     * Retrieve a map of pending ranges to endpoints that describe the ring topology
     * @param keyspace the keyspace to get the pending range map for.
     * @return a map of pending ranges to endpoints
     */
    public Map<Range, List<String>> getPendingRangeToEndpointMap(String keyspace);

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
     * @param key - key for which we need to find the endpoint return value -
     * the endpoint responsible for this key
     */
    public List<InetAddress> getNaturalEndpoints(String table, byte[] key);

    /**
     * Forces major compaction (all sstable files compacted)
     */
    public void forceTableCompaction() throws IOException;

    /**
     * Trigger a cleanup of keys on all tables.
     */
    public void forceTableCleanup() throws IOException;

    /**
     * Takes the snapshot for a given table.
     * 
     * @param tableName the name of the table.
     * @param tag       the tag given to the snapshot (null is permissible)
     */
    public void takeSnapshot(String tableName, String tag) throws IOException;

    /**
     * Takes a snapshot for every table.
     * 
     * @param tag the tag given to the snapshot (null is permissible)
     */
    public void takeAllSnapshot(String tag) throws IOException;

    /**
     * Remove all the existing snapshots.
     */
    public void clearSnapshot() throws IOException;

    /**
     * Flush all memtables for the given column families, or all columnfamilies for the given table
     * if none are explicitly listed.
     * @param tableName
     * @param columnFamilies
     * @throws IOException
     */
    public void forceTableFlush(String tableName, String... columnFamilies) throws IOException;

    /**
     * Triggers proactive repair for given column families, or all columnfamilies for the given table
     * if none are explicitly listed.
     * @param tableName
     * @param columnFamilies
     * @throws IOException
     */
    public void forceTableRepair(String tableName, String... columnFamilies) throws IOException;

    /**
     * transfer this node's data to other machines and remove it from service.
     */
    public void decommission() throws InterruptedException;

    /**
     * @param newToken token to move this node to.
     * This node will unload its data onto its neighbors, and bootstrap to the new token.
     */
    public void move(String newToken) throws IOException, InterruptedException;

    /**
     * This node will unload its data onto its neighbors, and bootstrap to share the range
     * of the most-loaded node in the ring.
     */
    public void loadBalance() throws IOException, InterruptedException;

    /**
     * removeToken removes token (and all data associated with
     * enpoint that had it) from the ring
     */
    public void removeToken(String token);

    /** set the logging level at runtime */
    public void setLog4jLevel(String classQualifier, String level);

    /** get the operational mode (leaving, joining, normal, decommissioned, client) **/
    public String getOperationMode();

    /** makes node unavailable for writes, flushes memtables and replays commitlog. */
    public void drain() throws IOException, InterruptedException, ExecutionException;

    /**
     * Introduced in 0.7 to allow nodes to load their existing yaml defined schemas.
     * @todo: deprecate in 0.7+1, remove in 0.7+2.
     */ 
    public void loadSchemaFromYAML() throws ConfigurationException, IOException;

    /**
     * Introduced in 0.7 to allow schema yaml to be exported.
     * @todo: deprecate in 0.7+1, remove in 0.7+2.
     */
    public String exportSchema() throws IOException;

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
}
