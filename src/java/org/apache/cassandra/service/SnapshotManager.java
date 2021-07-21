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


import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.apache.cassandra.db.SnapshotDetails;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.cassandra.config.Duration;
import org.apache.cassandra.db.Keyspace;

import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.cassandra.concurrent.ScheduledExecutors;

import static org.apache.cassandra.config.CassandraRelevantProperties.SNAPSHOT_CLEANUP_PERIOD_SECONDS;
import static org.apache.cassandra.config.CassandraRelevantProperties.SNAPSHOT_CLEANUP_INITIAL_DELAY_SECONDS;

public class SnapshotManager {
    private volatile ScheduledFuture snapshotCleanupTrigger;
    private static final Logger logger = LoggerFactory.getLogger(SnapshotManager.class);
    private Map<String, SnapshotDetails> activeTtlSnapshots = new ConcurrentHashMap();

    public void addTtlSnapshot(String tag, String table, String keyspace, Map<String, Object> manifest) {
        assert manifest.containsKey(SnapshotDetails.CREATED_AT);
        assert manifest.containsKey(SnapshotDetails.EXPIRES_AT);
        SnapshotDetails snapshot = new SnapshotDetails(tag, table, keyspace, manifest);
        activeTtlSnapshots.put(snapshot.keyspace + ":" + snapshot.tag, snapshot);
    }

    private void readSnapshotsFromDisk() {
        for (Keyspace ks : Keyspace.all()) {
            for (SnapshotDetails snapshot : ks.getSnapshotDetails()) {
                if (snapshot.hasTTL()) {
                    activeTtlSnapshots.put(snapshot.keyspace + ':' + snapshot.table + ':' + snapshot.tag, snapshot);
                }
            }
        }
    }

    public class SnapshotCleanupTrigger implements Runnable {
        private final Logger logger = LoggerFactory.getLogger(SnapshotCleanupTrigger.class);

        public void run() {
            logger.info("starting cleanup of expired snapshots");

            for (SnapshotDetails snapshot : activeTtlSnapshots.values()) {
                if (snapshot.isExpired()) {
                    Keyspace.clearSnapshot(snapshot.tag, snapshot.keyspace);
                    activeTtlSnapshots.remove(snapshot.keyspace + ':' + snapshot.table + ':' + snapshot.tag);
                }
            }
        }
    }

    public void startScanning() {
        logger.info("snapshot manager startup, scheduling cleanups");
        SnapshotCleanupTrigger trigger = new SnapshotCleanupTrigger();

        readSnapshotsFromDisk();

        snapshotCleanupTrigger = ScheduledExecutors.scheduledTasks.scheduleWithFixedDelay(
            trigger,
            SNAPSHOT_CLEANUP_INITIAL_DELAY_SECONDS.getInt(),
            SNAPSHOT_CLEANUP_PERIOD_SECONDS.getInt(),
            TimeUnit.SECONDS
        );
    }
}
