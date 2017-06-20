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

import com.google.common.util.concurrent.AbstractFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.MerkleTrees;

/**
 * SyncTask will calculate the difference of MerkleTree between two nodes
 * and perform necessary operation to repair replica.
 */
public abstract class SyncTask extends AbstractFuture<SyncStat> implements Runnable
{
    private static Logger logger = LoggerFactory.getLogger(SyncTask.class);

    protected final RepairJobDesc desc;
    protected final TreeResponse r1;
    protected final TreeResponse r2;
    protected final PreviewKind previewKind;

    protected volatile SyncStat stat;
    protected long startTime = Long.MIN_VALUE;

    public SyncTask(RepairJobDesc desc, TreeResponse r1, TreeResponse r2, PreviewKind previewKind)
    {
        this.desc = desc;
        this.r1 = r1;
        this.r2 = r2;
        this.previewKind = previewKind;
    }

    /**
     * Compares trees, and triggers repairs for any ranges that mismatch.
     */
    public void run()
    {
        startTime = System.currentTimeMillis();
        // compare trees, and collect differences
        List<Range<Token>> differences = MerkleTrees.difference(r1.trees, r2.trees);

        stat = new SyncStat(new NodePair(r1.endpoint, r2.endpoint), differences.size());

        // choose a repair method based on the significance of the difference
        String format = String.format("%s Endpoints %s and %s %%s for %s", previewKind.logPrefix(desc.sessionId), r1.endpoint, r2.endpoint, desc.columnFamily);
        if (differences.isEmpty())
        {
            logger.info(String.format(format, "are consistent"));
            Tracing.traceRepair("Endpoint {} is consistent with {} for {}", r1.endpoint, r2.endpoint, desc.columnFamily);
            set(stat);
            return;
        }

        // non-0 difference: perform streaming repair
        logger.info(String.format(format, "have " + differences.size() + " range(s) out of sync"));
        Tracing.traceRepair("Endpoint {} has {} range(s) out of sync with {} for {}", r1.endpoint, differences.size(), r2.endpoint, desc.columnFamily);
        startSync(differences);
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

    protected abstract void startSync(List<Range<Token>> differences);
}
