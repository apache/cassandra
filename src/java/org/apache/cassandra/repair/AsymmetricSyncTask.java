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

import java.net.InetAddress;
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

public abstract class AsymmetricSyncTask extends AbstractFuture<SyncStat> implements Runnable
{
    private static Logger logger = LoggerFactory.getLogger(AsymmetricSyncTask.class);
    protected final RepairJobDesc desc;
    protected final InetAddress fetchFrom;
    protected final List<Range<Token>> rangesToFetch;
    protected final InetAddress fetchingNode;
    protected final PreviewKind previewKind;
    private long startTime = Long.MIN_VALUE;
    protected volatile SyncStat stat;


    public AsymmetricSyncTask(RepairJobDesc desc, InetAddress fetchingNode, InetAddress fetchFrom, List<Range<Token>> rangesToFetch, PreviewKind previewKind)
    {
        this.desc = desc;
        this.fetchFrom = fetchFrom;
        this.fetchingNode = fetchingNode;
        this.rangesToFetch = rangesToFetch;
        // todo: make an AsymmetricSyncStat?
        stat = new SyncStat(new NodePair(fetchingNode, fetchFrom), rangesToFetch.size());
        this.previewKind = previewKind;
    }
    public void run()
    {
        startTime = System.currentTimeMillis();
        // choose a repair method based on the significance of the difference
        String format = String.format("%s Endpoints %s and %s %%s for %s", previewKind.logPrefix(desc.sessionId), fetchingNode, fetchFrom, desc.columnFamily);
        if (rangesToFetch.isEmpty())
        {
            logger.info(String.format(format, "are consistent"));
            Tracing.traceRepair("Endpoint {} is consistent with {} for {}", fetchingNode, fetchFrom, desc.columnFamily);
            set(stat);
            return;
        }

        // non-0 difference: perform streaming repair
        logger.info(String.format(format, "have " + rangesToFetch.size() + " range(s) out of sync"));
        Tracing.traceRepair("Endpoint {} has {} range(s) out of sync with {} for {}", fetchingNode, rangesToFetch.size(), fetchFrom, desc.columnFamily);
        startSync(rangesToFetch);
    }

    protected void finished()
    {
        if (startTime != Long.MIN_VALUE)
            Keyspace.open(desc.keyspace).getColumnFamilyStore(desc.columnFamily).metric.syncTime.update(System.currentTimeMillis() - startTime, TimeUnit.MILLISECONDS);
    }


    public abstract void startSync(List<Range<Token>> rangesToFetch);
}
