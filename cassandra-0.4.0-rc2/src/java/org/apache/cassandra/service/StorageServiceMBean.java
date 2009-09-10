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
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.net.EndPoint;


public interface StorageServiceMBean
{    
    public String getLiveNodes();
    public String getUnreachableNodes();
    public String getToken();
    public Map<Range, List<EndPoint>> getRangeToEndPointMap();
    public String getLoadInfo();
    public int getCurrentGenerationNumber();
    public void forceTableCompaction() throws IOException;
    
    /**
     * This method will cause the local node initiate
     * the bootstrap process for all the nodes specified
     * in the string parameter passed in. This local node
     * will calculate who gives what ranges to the nodes
     * and then instructs the nodes to do so.
     * 
     * @param nodes colon delimited list of endpoints that need
     *              to be bootstrapped
     * @throws UnknownHostException 
    */
    public void loadAll(String nodes) throws UnknownHostException;
    
    /**
     * 
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
