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

import org.apache.cassandra.dht.Range;
import org.apache.cassandra.net.EndPoint;


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
     * Fetch a string representation of the token.
     *
     * @return a string token
     */
    public String getToken();

    /**
     * Retrieve a map of range to end points that describe the ring topology
     * of a Cassandra cluster.
     *
     * @return mapping of ranges to end points
     */
    public Map<Range, List<String>> getRangeToEndPointMap();

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
     * Forces major compaction (all sstable files compacted)
     */
    public void forceTableCompaction() throws IOException;

    /**
     * Trigger a cleanup of keys on all tables.
     */
    public void forceTableCleanup() throws IOException;

    /**
     * Stream the files in the bootstrap directory over to the
     * node being bootstrapped. This is used in case of normal
     * bootstrap failure. Use a tool to re-calculate the cardinality
     * at a later point at the destination.
     * @param directories colon separated list of directories from where 
     *                files need to be picked up.
     * @param target endpoint receiving data.
    */
    public void forceHandoff(List<String> directories, String target) throws IOException;

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
     * Flush all binary memtables for a table
     * @param tableName
     * @throws IOException
     */
    public void forceTableFlushBinary(String tableName) throws IOException;

    /** set the logging level at runtime */
    public void setLog4jLevel(String classQualifier, String level);
}
