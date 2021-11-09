/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.db.compaction.unified;

import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.agrona.collections.IntArrayList;
import org.apache.cassandra.db.compaction.CompactionAggregate;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.config.CassandraRelevantProperties.UCS_COMPACTION_AGGREGATE_PRIORITIZER;

/**
 * Used to provide custom implementation to prioritize compaction tasks in UCS
 */
public interface CompactionAggregatePrioritizer
{
    Logger logger = LoggerFactory.getLogger(CompactionAggregatePrioritizer.class);

    CompactionAggregatePrioritizer instance = !UCS_COMPACTION_AGGREGATE_PRIORITIZER.isPresent()
                             ? new DefaultCompactionAggregatePrioritizer()
                             : FBUtilities.construct(UCS_COMPACTION_AGGREGATE_PRIORITIZER.getString(), "compaction aggregate prioritizer");

    /**
     * Maybe sort the provided pending compaction aggregates
     */
    List<CompactionAggregate.UnifiedAggregate> maybeSort(List<CompactionAggregate.UnifiedAggregate> pending);

    /**
     * Maybe reshuffle the provided aggregate indexes
     */
    IntArrayList maybeRandomize(IntArrayList aggregateIndexes, Random random);

    class DefaultCompactionAggregatePrioritizer implements CompactionAggregatePrioritizer
    {

        @Override
        public List<CompactionAggregate.UnifiedAggregate> maybeSort(List<CompactionAggregate.UnifiedAggregate> pending)
        {
            return pending;
        }

        @Override
        public IntArrayList maybeRandomize(IntArrayList aggregateIndexes, Random random)
        {
            Collections.shuffle(aggregateIndexes, random);
            return aggregateIndexes;
        }
    }
}
