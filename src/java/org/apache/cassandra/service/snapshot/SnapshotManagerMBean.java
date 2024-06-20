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

package org.apache.cassandra.service.snapshot;

import java.io.IOException;
import java.util.Map;
import javax.management.openmbean.TabularData;

public interface SnapshotManagerMBean
{
    String MBEAN_NAME = "org.apache.cassandra.service.snapshot:type=SnapshotManager";

    /**
     * Takes the snapshot of a multiple column family from different keyspaces. A snapshot name must be specified.
     *
     * @param tag      the tag given to the snapshot; may not be null or empty
     * @param options  Map of options (skipFlush is the only supported option for now)
     * @param entities list of keyspaces / tables in the form of empty | ks1 ks2 ... | ks1.cf1,ks2.cf2,...
     */
    void takeSnapshot(String tag, Map<String, String> options, String... entities) throws IOException;

    /**
     * Remove the snapshot with the given name from the given keyspaces.
     * If no tag is specified we will remove all snapshots.
     *
     * @param tag           name of snapshot to clear, if null or empty string, all snapshots of given keyspace will be cleared
     * @param options       map of options for cleanup operation, consult nodetool's ClearSnapshot
     * @param keyspaceNames name of keyspaces to clear snapshots for
     */
    void clearSnapshot(String tag, Map<String, Object> options, String... keyspaceNames) throws IOException;

    /**
     * Get the details of all the snapshots
     *
     * @param options map of options used for filtering of snapshots
     * @return A map of snapshotName to all its details in Tabular form.
     */
    Map<String, TabularData> listSnapshots(Map<String, String> options);

    /**
     * Get the true size taken by all snapshots across all keyspaces.
     *
     * @return True size taken by all the snapshots.
     */
    long getTrueSnapshotSize();

    /**
     * Set the current hardlink-per-second throttle for snapshots
     * A setting of zero indicates no throttling
     *
     * @param throttle hard-links-per-second
     */
    void setSnapshotLinksPerSecond(long throttle);

    /**
     * Get the current hardlink-per-second throttle for snapshots
     * A setting of zero indicates no throttling.
     *
     * @return snapshot links-per-second throttle
     */
    long getSnapshotLinksPerSecond();

    /**
     * Restarting means that snapshots will be reloaded from disk.
     */
    void restart();
}