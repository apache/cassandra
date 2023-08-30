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
package org.apache.cassandra.repair;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.google.common.base.Preconditions;

import org.apache.cassandra.concurrent.ExecutorPlus;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.metrics.RepairMetrics;
import org.apache.cassandra.repair.consistent.SyncStatSummary;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.utils.DiagnosticSnapshotService;
import org.apache.cassandra.utils.TimeUUID;
import org.apache.cassandra.utils.concurrent.Future;

public class PreviewRepairTask extends AbstractRepairTask
{
    private final TimeUUID parentSession;
    private final List<CommonRange> commonRanges;
    private final String[] cfnames;
    private volatile String successMessage = name() + " completed successfully";

    protected PreviewRepairTask(RepairCoordinator coordinator, TimeUUID parentSession, List<CommonRange> commonRanges, String[] cfnames)
    {
        super(coordinator);
        this.parentSession = parentSession;
        this.commonRanges = commonRanges;
        this.cfnames = cfnames;
    }

    @Override
    public String name()
    {
        return "Repair preview";
    }

    @Override
    public String successMessage()
    {
        return successMessage;
    }

    @Override
    public Future<CoordinatedRepairResult> performUnsafe(ExecutorPlus executor)
    {
        Future<CoordinatedRepairResult> f = runRepair(parentSession, false, executor, commonRanges, cfnames);
        return f.map(result -> {
            if (result.hasFailed())
                return result;

            PreviewKind previewKind = options.getPreviewKind();
            Preconditions.checkState(previewKind != PreviewKind.NONE, "Preview is NONE");
            SyncStatSummary summary = new SyncStatSummary(true);
            summary.consumeSessionResults(result.results);

            final String message;
            if (summary.isEmpty())
            {
                message = previewKind == PreviewKind.REPAIRED ? "Repaired data is in sync" : "Previewed data was in sync";
            }
            else
            {
                message = (previewKind == PreviewKind.REPAIRED ? "Repaired data is inconsistent\n" : "Preview complete\n") + summary;
                RepairMetrics.previewFailures.inc();
                if (previewKind == PreviewKind.REPAIRED)
                    maybeSnapshotReplicas(parentSession, keyspace, result.results.get()); // we know its present as summary used it
            }
            successMessage += "; " + message;
            coordinator.notification(message);

            return result;
        });
    }

    private void maybeSnapshotReplicas(TimeUUID parentSession, String keyspace, List<RepairSessionResult> results)
    {
        if (!DatabaseDescriptor.snapshotOnRepairedDataMismatch())
            return;

        try
        {
            Set<String> mismatchingTables = new HashSet<>();
            Set<InetAddressAndPort> nodes = new HashSet<>();
            Set<Range<Token>> ranges = new HashSet<>();
            for (RepairSessionResult sessionResult : results)
            {
                for (RepairResult repairResult : emptyIfNull(sessionResult.repairJobResults))
                {
                    for (SyncStat stat : emptyIfNull(repairResult.stats))
                    {
                        if (!stat.differences.isEmpty())
                        {
                            mismatchingTables.add(repairResult.desc.columnFamily);
                            ranges.addAll(stat.differences);
                        }
                        // snapshot all replicas, even if they don't have any differences
                        nodes.add(stat.nodes.coordinator);
                        nodes.add(stat.nodes.peer);
                    }
                }
            }

            String snapshotName = DiagnosticSnapshotService.getSnapshotName(DiagnosticSnapshotService.REPAIRED_DATA_MISMATCH_SNAPSHOT_PREFIX);
            for (String table : mismatchingTables)
            {
                // we can just check snapshot existence locally since the repair coordinator is always a replica (unlike in the read case)
                if (!Keyspace.open(keyspace).getColumnFamilyStore(table).snapshotExists(snapshotName))
                {
                    List<Range<Token>> normalizedRanges = Range.normalize(ranges);
                    logger.info("{} Snapshotting {}.{} for preview repair mismatch for ranges {} with tag {} on instances {}",
                                options.getPreviewKind().logPrefix(parentSession),
                                keyspace, table, normalizedRanges, snapshotName, nodes);
                    DiagnosticSnapshotService.repairedDataMismatch(Keyspace.open(keyspace).getColumnFamilyStore(table).metadata(),
                                                                   nodes,
                                                                   normalizedRanges);
                }
                else
                {
                    logger.info("{} Not snapshotting {}.{} - snapshot {} exists",
                                options.getPreviewKind().logPrefix(parentSession),
                                keyspace, table, snapshotName);
                }
            }
        }
        catch (Exception e)
        {
            logger.error("{} Failed snapshotting replicas", options.getPreviewKind().logPrefix(parentSession), e);
        }
    }

    private static <T> Iterable<T> emptyIfNull(Iterable<T> iter)
    {
        if (iter == null)
            return Collections.emptyList();
        return iter;
    }
}
