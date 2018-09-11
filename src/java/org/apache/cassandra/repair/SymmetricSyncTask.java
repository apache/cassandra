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

import java.util.List;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.tracing.Tracing;

/**
 * SymmetricSyncTask will calculate the difference of MerkleTree between two nodes
 * and perform necessary operation to repair replica.
 */
public abstract class SymmetricSyncTask extends AbstractSyncTask
{
    private static Logger logger = LoggerFactory.getLogger(SymmetricSyncTask.class);

    protected final RepairJobDesc desc;
    protected final InetAddressAndPort endpoint1;
    protected final InetAddressAndPort endpoint2;
    protected final List<Range<Token>> differences;
    protected final PreviewKind previewKind;
    protected final NodePair nodePair;
    protected volatile SyncStat stat;
    protected long startTime = Long.MIN_VALUE;

    public SymmetricSyncTask(RepairJobDesc desc, InetAddressAndPort endpoint1, InetAddressAndPort endpoint2, List<Range<Token>> differences, PreviewKind previewKind)
    {
        Preconditions.checkArgument(!endpoint1.equals(endpoint2), "Both sync targets are the same: %s", endpoint1);
        this.desc = desc;
        this.endpoint1 = endpoint1;
        this.endpoint2 = endpoint2;
        this.differences = differences;
        this.previewKind = previewKind;
        this.nodePair = new NodePair(endpoint1, endpoint2);
    }

    /**
     * Compares trees, and triggers repairs for any ranges that mismatch.
     */
    public void run()
    {
        startTime = System.currentTimeMillis();

        stat = new SyncStat(nodePair, differences.size());

        // choose a repair method based on the significance of the difference
        String format = String.format("%s Endpoints %s and %s %%s for %s", previewKind.logPrefix(desc.sessionId), endpoint1, endpoint2, desc.columnFamily);
        if (differences.isEmpty())
        {
            logger.info(String.format(format, "are consistent"));
            Tracing.traceRepair("Endpoint {} is consistent with {} for {}", endpoint1, endpoint2, desc.columnFamily);
            set(stat);
            return;
        }

        // non-0 difference: perform streaming repair
        logger.info(String.format(format, "have " + differences.size() + " range(s) out of sync"));
        Tracing.traceRepair("Endpoint {} has {} range(s) out of sync with {} for {}", endpoint1, differences.size(), endpoint2, desc.columnFamily);
        startSync(differences);
    }

    public NodePair nodePair()
    {
        return nodePair;
    }

    public SyncStat getCurrentStat()
    {
        return stat;
    }

    protected void finished()
    {
        if (startTime != Long.MIN_VALUE)
            Keyspace.open(desc.keyspace).getColumnFamilyStore(desc.columnFamily).metric.syncTime.update(System.currentTimeMillis() - startTime, TimeUnit.MILLISECONDS);
    }
}
