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
package org.apache.cassandra.service.reads.trackwarnings;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.util.concurrent.FastThreadLocal;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.metrics.TableMetrics;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.service.ClientWarn;
import org.apache.cassandra.service.reads.ReadCallback;

public class Shared
{
    private static final FastThreadLocal<Map<ReadCommand, TrackWarningsSnapshot>> state = new FastThreadLocal<>();
    private static final FastThreadLocal<Throwable> initter = new FastThreadLocal<>();

    private Shared() {}

    public static boolean isActive()
    {
        return state.get() != null;
    }

    public static void init()
    {
        if (state.get() != null)
            throw new AssertionError("Share.init called while state is not null: " + state.get(), initter.get());
        state.set(new HashMap<>());
        initter.set(new Throwable());
        logger.error("Share.init called in thread {}", Thread.currentThread().getName());
    }

    public static void clean()
    {
        state.set(null);
        initter.set(null);
    }

    public static void update(ReadCommand cmd, TrackWarningsSnapshot snapshot)
    {
        Map<ReadCommand, TrackWarningsSnapshot> map = state.get();
        if (map == null)
            throw new AssertionError("Attempted to update Shared state without first calling #init");

        TrackWarningsSnapshot previous = map.get(cmd);
        TrackWarningsSnapshot update = TrackWarningsSnapshot.merge(previous, snapshot);
        logger.error("update on thread {}", Thread.currentThread().getName());
        if (update == null)
            map.remove(cmd);
        else
            map.put(cmd, update);
    }

    private interface ToString
    {
        String apply(int count, long value, String cql);
    }

    public static void done()
    {
        Map<ReadCommand, TrackWarningsSnapshot> map = state.get();
        if (map == null)
            throw new AssertionError("Attempted to call done in Shared state without first calling #init");

        logger.error("done on thread {} with size of {}", Thread.currentThread().getName(), map.size());

        map.forEach((command, merged) -> {
            String cql = command.toCQLString();
            String loggableTokens = command.loggableTokens();

            ColumnFamilyStore cfs = cfs(command);
            if (cfs == null)
                return;
            trackAborts(merged.tombstones, cql, loggableTokens, cfs.metric.clientTombstoneAborts, ReadCallback::tombstoneAbortMessage);
            trackWarnings(merged.tombstones, cql, loggableTokens, cfs.metric.clientTombstoneWarnings, ReadCallback::tombstoneWarnMessage);

            trackAborts(merged.localReadSize, cql, loggableTokens, cfs.metric.localReadSizeAborts, ReadCallback::localReadSizeAbortMessage);
            trackWarnings(merged.localReadSize, cql, loggableTokens, cfs.metric.localReadSizeWarnings, ReadCallback::localReadSizeWarnMessage);

            trackAborts(merged.rowIndexTooLarge, cql, loggableTokens, cfs.metric.rowIndexSizeAborts, ReadCallback::rowIndexSizeAbortMessage);
            trackWarnings(merged.rowIndexTooLarge, cql, loggableTokens, cfs.metric.rowIndexSizeWarnings, ReadCallback::rowIndexSizeWarnMessage);
        });

        clean();
    }

    private static final Logger logger = LoggerFactory.getLogger(Shared.class);

    private static void trackAborts(TrackWarningsSnapshot.Warnings counter, String cql, String loggableTokens, TableMetrics.TableMeter metric, ToString toString)
    {
        if (counter.aborts.count > 0)
        {
            String msg = toString.apply(counter.aborts.count, counter.aborts.maxValue, cql);
            ClientWarn.instance.warn(msg + " with " + loggableTokens);
            logger.warn(msg);
            metric.mark();
        }
    }

    private static void trackWarnings(TrackWarningsSnapshot.Warnings counter, String cql, String loggableTokens, TableMetrics.TableMeter metric, ToString toString)
    {
        if (counter.warnings.count > 0)
        {
            String msg = toString.apply(counter.warnings.count, counter.warnings.maxValue, cql);
            ClientWarn.instance.warn(msg + " with " + loggableTokens);
            logger.warn(msg);
            metric.mark();
        }
    }

    private static ColumnFamilyStore cfs(ReadCommand command)
    {
        return Schema.instance.getColumnFamilyStoreInstance(command.metadata().id);
    }
}
