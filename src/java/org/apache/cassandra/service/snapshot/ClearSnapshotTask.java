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

import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.DurationSpec;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.utils.Clock;

public class ClearSnapshotTask extends AbstractSnapshotTask<Void>
{
    private static final Logger logger = LoggerFactory.getLogger(ClearSnapshotTask.class);

    private final SnapshotManager manager;
    private final Predicate<TableSnapshot> predicateForToBeCleanedSnapshots;
    private final boolean deleteData;

    public ClearSnapshotTask(SnapshotManager manager,
                             Predicate<TableSnapshot> predicateForToBeCleanedSnapshots,
                             boolean deleteData)
    {
        super(null);
        this.manager = manager;
        this.predicateForToBeCleanedSnapshots = predicateForToBeCleanedSnapshots;
        this.deleteData = deleteData;
    }

    @Override
    public SnapshotTaskType getTaskType()
    {
        return SnapshotTaskType.CLEAR;
    }

    @Override
    public Void call()
    {
        Set<TableSnapshot> toRemove = new HashSet<>();

        for (TableSnapshot snapshot : new GetSnapshotsTask(manager, predicateForToBeCleanedSnapshots, false).call())
        {
            logger.debug("Removing snapshot {}{}", snapshot, deleteData ? ", deleting data" : "");

            toRemove.add(snapshot);

            if (deleteData)
            {
                for (File snapshotDir : snapshot.getDirectories())
                {
                    try
                    {
                        removeSnapshotDirectory(snapshotDir);
                    }
                    catch (Throwable ex)
                    {
                        logger.warn("Unable to remove snapshot directory {}", snapshotDir, ex);
                    }
                }
            }
        }

        manager.getSnapshots().removeAll(toRemove);

        return null;
    }

    /**
     * Returns predicate which will pass the test when arguments match.
     *
     * @param tag name of snapshot
     * @param options options for filtering
     * @param keyspaceNames names of keyspaces a snapshot is supposed to be from
     * @return predicate which will pass the test when arguments match.
     */
    static Predicate<TableSnapshot> getPredicateForCleanedSnapshots(String tag, Map<String, Object> options, String... keyspaceNames)
    {
        if (options == null)
            options = Collections.emptyMap();

        Object olderThan = options.get("older_than");
        Object olderThanTimestamp = options.get("older_than_timestamp");

        long maxCreatedAt = Clock.Global.currentTimeMillis();
        if (olderThan != null)
        {
            assert olderThan instanceof String : "it is expected that older_than is an instance of java.lang.String";
            maxCreatedAt -= new DurationSpec.LongSecondsBound((String) olderThan).toMilliseconds();
        }
        else if (olderThanTimestamp != null)
        {
            assert olderThanTimestamp instanceof String : "it is expected that older_than_timestamp is an instance of java.lang.String";
            try
            {
                maxCreatedAt = Instant.parse((String) olderThanTimestamp).toEpochMilli();
            }
            catch (DateTimeParseException ex)
            {
                throw new RuntimeException("Parameter older_than_timestamp has to be a valid instant in ISO format.");
            }
        }

        return getClearSnapshotPredicate(tag, Set.of(keyspaceNames), maxCreatedAt, false);
    }

    /**
     * Returns a predicate based on which a snapshot will be included for deletion or not.
     *
     * @param tag                name of snapshot to remove
     * @param keyspaces          keyspaces this snapshot belongs to
     * @param olderThanTimestamp clear the snapshot if it is older than given timestamp
     * @param includeEphemeral   whether to include ephemeral snapshots as well
     * @return predicate which filters snapshots on given parameters
     */
    static Predicate<TableSnapshot> getClearSnapshotPredicate(String tag,
                                                              Set<String> keyspaces,
                                                              long olderThanTimestamp,
                                                              boolean includeEphemeral)
    {
        return ts ->
        {
            // When no tag is supplied, all snapshots must be cleared
            boolean clearAll = tag == null || tag.isEmpty();
            if (!clearAll && ts.isEphemeral() && !includeEphemeral)
                logger.info("Skipping deletion of ephemeral snapshot '{}' in keyspace {}. " +
                            "Ephemeral snapshots are not removable by a user.",
                            tag, ts.getKeyspaceName());
            boolean passedEphemeralTest = !ts.isEphemeral() || (ts.isEphemeral() && includeEphemeral);
            boolean shouldClearTag = clearAll || ts.getTag().equals(tag);
            boolean byTimestamp = true;

            if (olderThanTimestamp > 0L)
            {
                Instant createdAt = ts.getCreatedAt();
                if (createdAt != null)
                    byTimestamp = createdAt.isBefore(Instant.ofEpochMilli(olderThanTimestamp));
            }

            boolean byKeyspace = (keyspaces.isEmpty() || keyspaces.contains(ts.getKeyspaceName()));

            return passedEphemeralTest && shouldClearTag && byTimestamp && byKeyspace;
        };
    }

    private void removeSnapshotDirectory(File snapshotDir)
    {
        if (snapshotDir.exists())
        {
            logger.trace("Removing snapshot directory {}", snapshotDir);
            try
            {

                snapshotDir.deleteRecursive(DatabaseDescriptor.getSnapshotRateLimiter());
            }
            catch (RuntimeException ex)
            {
                if (!snapshotDir.exists())
                    return; // ignore
                throw ex;
            }
        }
    }

    @Override
    public String toString()
    {
        return "ClearSnapshotTask{" +
               "deleteData=" + deleteData +
               '}';
    }
}
