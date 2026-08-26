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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.cassandra.schema;

import java.util.Map;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.Test;

import org.apache.cassandra.db.compaction.AbstractCompactionStrategy;
import org.apache.cassandra.db.compaction.LeveledCompactionStrategy;
import org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy;
import org.apache.cassandra.db.compaction.TimeWindowCompactionStrategy;
import org.apache.cassandra.db.compaction.UnifiedCompactionStrategy;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.utils.ClassLoadingTestNonAssignable;
import org.apache.cassandra.utils.ClassLoadingTestSupport;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class CompactionParamsTest
{
    private static final String MIN_SSTABLE_SIZE = "min_sstable_size";

    /**
     * We persist only the plain byte count in the schema to ensure we keep compatibility
     * previous cassandra versions that didn't support storage units.
     */
    @Test
    public void testMinSSTableSizeIsNormalizedBeforeSerialization()
    {
        for (Class<? extends AbstractCompactionStrategy> klass : ImmutableList.of(SizeTieredCompactionStrategy.class,
                                                                                 LeveledCompactionStrategy.class,
                                                                                 TimeWindowCompactionStrategy.class))
        {
            for (String written : ImmutableList.of("50MiB", "50 MiB", "51200KiB", "52428800"))
            {
                CompactionParams params = CompactionParams.create(klass, ImmutableMap.of(MIN_SSTABLE_SIZE, written));
                params.validate();
                assertThat(params.options()).containsEntry(MIN_SSTABLE_SIZE, "52428800");
                assertThat(params.asMap()).containsEntry(MIN_SSTABLE_SIZE, "52428800");
            }

            // an unparsable value is left untouched so that validate() can reject it with a proper message
            CompactionParams invalid = CompactionParams.create(klass, ImmutableMap.of(MIN_SSTABLE_SIZE, "50Mi"));
            assertThat(invalid.options()).containsEntry(MIN_SSTABLE_SIZE, "50Mi");
            assertThatThrownBy(invalid::validate).isInstanceOf(ConfigurationException.class)
                                                 .hasMessageContaining(MIN_SSTABLE_SIZE);
        }
    }

    /**
     * UCS parses its size options with {@link org.apache.cassandra.utils.FBUtilities#parseHumanReadableBytes}, which
     * requires the unit suffix, so its options must not be rewritten into a bare byte count.
     */
    @Test
    public void testUnifiedCompactionStrategyOptionsAreNotNormalized()
    {
        Map<String, String> options = ImmutableMap.of(MIN_SSTABLE_SIZE, "50MiB", "target_sstable_size", "1GiB");
        CompactionParams params = CompactionParams.create(UnifiedCompactionStrategy.class, options);
        params.validate();
        assertThat(params.options()).containsEntry(MIN_SSTABLE_SIZE, "50MiB");
    }

    @Test
    public void testRejectsNonCompactionStrategyWithoutInitializing()
    {
        ClassLoadingTestSupport.assertNotInitialized(ClassLoadingTestNonAssignable.class);

        assertThatThrownBy(() -> CompactionParams.classFromName(ClassLoadingTestNonAssignable.class.getName()))
        .isInstanceOf(ConfigurationException.class)
        .hasMessageContaining("must extend or implement " + AbstractCompactionStrategy.class.getName());

        assertThat(ClassLoadingTestSupport.wasInitialized(ClassLoadingTestNonAssignable.class)).isFalse();
    }
}
