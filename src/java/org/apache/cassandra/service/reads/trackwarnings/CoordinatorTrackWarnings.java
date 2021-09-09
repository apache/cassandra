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

import java.util.AbstractMap;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.util.concurrent.FastThreadLocal;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.metrics.TableMetrics;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.service.ClientWarn;
import org.apache.cassandra.service.reads.ReadCallback;

public class CoordinatorTrackWarnings
{
    private static final Logger logger = LoggerFactory.getLogger(CoordinatorTrackWarnings.class);
    private static final boolean ENABLE_DEFENSIVE_CHECKS = Boolean.getBoolean("cassandra.track_warnings.coordinator.devensive_checks_enabled");

    // when .init() is called set the STATE to be INIT; this is to lazy allocate the map only when warnings are generated
    private static final Map<ReadCommand, TrackWarningsSnapshot> INIT = Collections.emptyMap();
    private static final FastThreadLocal<Map<ReadCommand, TrackWarningsSnapshot>> STATE = new FastThreadLocal<>();

    private CoordinatorTrackWarnings() {}

    public static void init()
    {
        logger.trace("CoordinatorTrackWarnings.init()");
        if (STATE.get() != null)
        {
            if (ENABLE_DEFENSIVE_CHECKS)
                throw new AssertionError("CoordinatorTrackWarnings.init called while state is not null: " + STATE.get());
            return;
        }
        STATE.set(INIT);
    }

    public static void update(ReadCommand cmd, TrackWarningsSnapshot snapshot)
    {
        logger.trace("CoordinatorTrackWarnings.update({}, {})", cmd.metadata(), snapshot);
        Map<ReadCommand, TrackWarningsSnapshot> map = mutable();
        TrackWarningsSnapshot previous = map.get(cmd);
        TrackWarningsSnapshot update = TrackWarningsSnapshot.merge(previous, snapshot);
        if (update == null) // null happens when the merge had null input or EMPTY input... remove the command from the map
            map.remove(cmd);
        else
            map.put(cmd, update);
    }

    public static void done()
    {
        Map<ReadCommand, TrackWarningsSnapshot> map = readonly();
        logger.trace("CoordinatorTrackWarnings.done() with state {}", map);
        map.forEach((command, merged) -> {
            ColumnFamilyStore cfs = Schema.instance.getColumnFamilyStoreInstance(command.metadata().id);
            // race condition when dropping tables, also happens in unit tests as Schema may be bypassed
            if (cfs == null)
                return;

            String cql = command.toCQLString();
            String loggableTokens = command.loggableTokens();
            trackAborts(merged.tombstones, cql, loggableTokens, cfs.metric.clientTombstoneAborts, ReadCallback::tombstoneAbortMessage);
            trackWarnings(merged.tombstones, cql, loggableTokens, cfs.metric.clientTombstoneWarnings, ReadCallback::tombstoneWarnMessage);

            trackAborts(merged.localReadSize, cql, loggableTokens, cfs.metric.localReadSizeAborts, ReadCallback::localReadSizeAbortMessage);
            trackWarnings(merged.localReadSize, cql, loggableTokens, cfs.metric.localReadSizeWarnings, ReadCallback::localReadSizeWarnMessage);

            trackAborts(merged.rowIndexTooLarge, cql, loggableTokens, cfs.metric.rowIndexSizeAborts, ReadCallback::rowIndexSizeAbortMessage);
            trackWarnings(merged.rowIndexTooLarge, cql, loggableTokens, cfs.metric.rowIndexSizeWarnings, ReadCallback::rowIndexSizeWarnMessage);
        });

        STATE.set(null);
    }

    private static Map<ReadCommand, TrackWarningsSnapshot> mutable()
    {
        Map<ReadCommand, TrackWarningsSnapshot> map = STATE.get();
        if (map == null)
        {
            if (ENABLE_DEFENSIVE_CHECKS)
                throw new AssertionError("CoordinatorTrackWarnings.mutable calling without calling .init() first");
            // set map to an "ignore" map; dropping all mutations
            // this does not update the state as it is not known if .clear() will be called; so safter to leave state null
            map = IgnoreMap.get();
        }
        if (map == INIT)
        {
            map = new HashMap<>();
            STATE.set(map);
        }
        return map;
    }

    private static Map<ReadCommand, TrackWarningsSnapshot> readonly()
    {
        Map<ReadCommand, TrackWarningsSnapshot> map = STATE.get();
        if (map == null)
        {
            if (ENABLE_DEFENSIVE_CHECKS)
                throw new AssertionError("CoordinatorTrackWarnings.readonly calling without calling .init() first");
            // set map to an "ignore" map; dropping all mutations
            // this does not update the state as it is not known if .clear() will be called; so safter to leave state null
            map = IgnoreMap.get();
        }
        return map;
    }

    // utility interface to let callers use static functions
    @FunctionalInterface
    private interface ToString
    {
        String apply(int count, long value, String cql);
    }

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

    /**
     * Utility class to create an immutable map which does not fail on mutation but instead ignores it.
     */
    private static final class IgnoreMap extends AbstractMap<Object, Object>
    {
        private static final IgnoreMap INSTANCE = new IgnoreMap();

        private static <K, V> Map<K, V> get()
        {
            return (Map<K, V>) INSTANCE;
        }

        @Override
        public Object put(Object key, Object value)
        {
            return null;
        }

        @Override
        public Set<Entry<Object, Object>> entrySet()
        {
            return Collections.emptySet();
        }
    }
}
