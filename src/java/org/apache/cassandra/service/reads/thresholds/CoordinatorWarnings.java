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
package org.apache.cassandra.service.reads.thresholds;

import java.util.AbstractMap;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.metrics.TableMetrics;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.service.ClientWarn;
import org.apache.cassandra.service.thresholds.CoordinatorWarningsState;

import static org.apache.cassandra.config.CassandraRelevantProperties.READS_THRESHOLDS_COORDINATOR_DEFENSIVE_CHECKS_ENABLED;

/**
 * ThreadLocal manager for read warnings at the coordinator.
 * <p>
 * REFACTORED VERSION: Uses CoordinatorWarningsState for state management.
 */
public class CoordinatorWarnings
{
    private static final Logger logger = LoggerFactory.getLogger(CoordinatorWarnings.class);
    private static final boolean ENABLE_DEFENSIVE_CHECKS = READS_THRESHOLDS_COORDINATOR_DEFENSIVE_CHECKS_ENABLED.getBoolean();

    private static final Map<ReadCommand, WarningsSnapshot> INIT = Collections.emptyMap();

    private static final CoordinatorWarningsState<Map<ReadCommand, WarningsSnapshot>> STATE =
    new CoordinatorWarningsState<>("CoordinatorWarnings",
                                   INIT,
                                   null, // use remove() instead of setting to null
                                   HashMap::new,
                                   logger,
                                   ENABLE_DEFENSIVE_CHECKS);

    private CoordinatorWarnings() {}

    public static void init()
    {
        STATE.init();
    }

    public static void reset()
    {
        STATE.reset();
    }

    public static void update(ReadCommand cmd, WarningsSnapshot snapshot)
    {
        if (logger.isTraceEnabled())
            logger.trace("CoordinatorTrackWarnings.update({}, {})", cmd.metadata(), snapshot);

        Map<ReadCommand, WarningsSnapshot> map = STATE.getMutableState(IgnoreMap::get);

        WarningsSnapshot previous = map.get(cmd);
        WarningsSnapshot update = WarningsSnapshot.merge(previous, snapshot);

        if (update == null) // null happens when the merge had null input or EMPTY input
            map.remove(cmd);
        else
            map.put(cmd, update);
    }

    public static void done()
    {
        STATE.processAndReset(
        map -> map != INIT && !map.isEmpty(),
        CoordinatorWarnings::processWarnings,
        null // use default error handler
        );
    }

    private static void processWarnings(Map<ReadCommand, WarningsSnapshot> map)
    {
        if (logger.isTraceEnabled())
            logger.trace("CoordinatorTrackWarnings.done() with state {}", map);

        map.forEach((command, merged) -> {
            ColumnFamilyStore cfs = Schema.instance.getColumnFamilyStoreInstance(command.metadata().id);
            // race condition when dropping tables, also happens in unit tests as Schema may be bypassed
            if (cfs == null)
                return;

            String cql = command.toCQLString();
            String loggableTokens = command.loggableTokens();

            recordAborts(merged.tombstones, cql, loggableTokens, cfs.metric.clientTombstoneAborts, WarningsSnapshot::tombstoneAbortMessage);
            recordWarnings(merged.tombstones, cql, loggableTokens, cfs.metric.clientTombstoneWarnings, WarningsSnapshot::tombstoneWarnMessage);

            recordAborts(merged.localReadSize, cql, loggableTokens, cfs.metric.localReadSizeAborts, WarningsSnapshot::localReadSizeAbortMessage);
            recordWarnings(merged.localReadSize, cql, loggableTokens, cfs.metric.localReadSizeWarnings, WarningsSnapshot::localReadSizeWarnMessage);

            recordAborts(merged.rowIndexReadSize, cql, loggableTokens, cfs.metric.rowIndexSizeAborts, WarningsSnapshot::rowIndexReadSizeAbortMessage);
            recordWarnings(merged.rowIndexReadSize, cql, loggableTokens, cfs.metric.rowIndexSizeWarnings, WarningsSnapshot::rowIndexSizeWarnMessage);

            recordAborts(merged.indexReadSSTablesCount, cql, loggableTokens, cfs.metric.tooManySSTableIndexesReadAborts, WarningsSnapshot::tooManyIndexesReadAbortMessage);
            recordWarnings(merged.indexReadSSTablesCount, cql, loggableTokens, cfs.metric.tooManySSTableIndexesReadWarnings, WarningsSnapshot::tooManyIndexesReadWarnMessage);
        });
    }

    @FunctionalInterface
    private interface ToString
    {
        String apply(int count, long value, String cql);
    }

    private static void recordAborts(WarningsSnapshot.Warnings counter, String cql, String loggableTokens, TableMetrics.TableMeter metric, ToString toString)
    {
        if (!counter.aborts.instances.isEmpty())
        {
            String msg = toString.apply(counter.aborts.instances.size(), counter.aborts.maxValue, cql);
            ClientWarn.instance.warn(msg + " with " + loggableTokens);
            logger.warn(msg);
            metric.mark();
        }
    }

    private static void recordWarnings(WarningsSnapshot.Warnings counter, String cql, String loggableTokens, TableMetrics.TableMeter metric, ToString toString)
    {
        if (!counter.warnings.instances.isEmpty())
        {
            String msg = toString.apply(counter.warnings.instances.size(), counter.warnings.maxValue, cql);
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