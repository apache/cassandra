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

package org.apache.cassandra.tcm.transformations;

import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableMap;
import org.junit.Test;

import org.apache.cassandra.db.guardrails.CustomGuardrailConfig;
import org.apache.cassandra.db.guardrails.validators.CassandraPasswordValidator.CassandraPasswordValidatorConfiguration;
import org.apache.cassandra.tcm.GuardrailsMetadata;
import org.apache.cassandra.tcm.GuardrailsMetadata.ValuesHolder;
import org.apache.cassandra.tcm.transformations.GuardrailTransformations.Custom;
import org.apache.cassandra.tcm.transformations.GuardrailTransformations.Flag;
import org.apache.cassandra.tcm.transformations.GuardrailTransformations.Thresholds;
import org.apache.cassandra.tcm.transformations.GuardrailTransformations.Values;
import org.apache.cassandra.utils.Pair;

import static org.apache.cassandra.db.ConsistencyLevel.EACH_QUORUM;
import static org.apache.cassandra.db.ConsistencyLevel.ONE;
import static org.apache.cassandra.db.ConsistencyLevel.QUORUM;
import static org.apache.cassandra.db.ConsistencyLevel.THREE;
import static org.apache.cassandra.db.ConsistencyLevel.TWO;
import static org.apache.cassandra.db.guardrails.validators.CassandraPasswordValidator.CassandraPasswordValidatorConfiguration.ILLEGAL_SEQUENCE_LENGTH_KEY;
import static org.apache.cassandra.tcm.Epoch.EMPTY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class GuardrailsMetadataTest
{
    @Test
    public void testDiffingFlags()
    {
        GuardrailsMetadata oldMetadata = new GuardrailsMetadata(EMPTY);
        assertEquals(0, oldMetadata.getFlags().size());
        GuardrailsMetadata newMetadata = oldMetadata.with(new Flag("simplestrategy", false));
        ImmutableMap<String, Boolean> diff = oldMetadata.diffFlags(newMetadata);
        assertNotNull(diff);
        assertEquals(1, diff.size());
        assertThat(diff.get("simplestrategy")).isNotNull().isFalse();
        oldMetadata = newMetadata;

        newMetadata = oldMetadata.with(new Flag("simplestrategy", false));
        diff = oldMetadata.diffFlags(newMetadata);
        assertNotNull(diff);
        assertEquals(0, diff.size());
        assertThat(diff.get("simplestrategy")).isNull();
        oldMetadata = newMetadata;

        newMetadata = oldMetadata.with(new Flag("simplestrategy", true));
        diff = oldMetadata.diffFlags(newMetadata);
        assertNotNull(diff);
        assertEquals(1, diff.size());
        assertThat(diff.get("simplestrategy")).isNotNull().isTrue();
        oldMetadata = newMetadata;

        newMetadata = oldMetadata.with(new Flag("intersect_filtering_query", false));
        diff = oldMetadata.diffFlags(newMetadata);
        assertNotNull(diff);
        assertEquals(1, diff.size());

        assertThat(newMetadata.getFlags().get("intersect_filtering_query")).isNotNull();
        assertThat(newMetadata.getFlags().get("simplestrategy")).isNotNull();

        assertEquals(2, newMetadata.getFlags().size());
    }

    @Test
    public void testDiffingThresholds()
    {
        GuardrailsMetadata oldMetadata = new GuardrailsMetadata(EMPTY);
        assertEquals(0, oldMetadata.getThresholds().size());
        GuardrailsMetadata newMetadata = oldMetadata.with(new Thresholds("column_value_size", 10, 20));

        ImmutableMap<String, Pair<Long, Long>> diff = oldMetadata.diffThresholds(newMetadata);
        assertNotNull(diff);
        assertEquals(1, diff.size());
        Pair<Long, Long> diffThresholds = diff.get("column_value_size");
        assertEquals(Pair.create(10L, 20L), diffThresholds);

        oldMetadata = newMetadata;

        newMetadata = oldMetadata.with(new Thresholds("column_value_size", 10, 20));

        diff = oldMetadata.diffThresholds(newMetadata);
        assertNotNull(diff);
        assertEquals(0, diff.size());

        oldMetadata = newMetadata;

        newMetadata = oldMetadata.with(new Thresholds("column_value_size", 10, 30));
        diff = oldMetadata.diffThresholds(newMetadata);
        assertNotNull(diff);
        assertEquals(1, diff.size());
        diffThresholds = diff.get("column_value_size");
        assertEquals(Pair.create(10L, 30L), diffThresholds);

        oldMetadata = newMetadata;

        assertEquals(1, newMetadata.getThresholds().size());

        newMetadata = oldMetadata.with(new Thresholds("collection_size", 100, 150));
        diff = oldMetadata.diffThresholds(newMetadata);
        assertNotNull(diff);
        assertEquals(1, diff.size());
        diffThresholds = diff.get("collection_size");
        assertEquals(Pair.create(100L, 150L), diffThresholds);

        assertEquals(2, newMetadata.getThresholds().size());

        Thresholds t1 = newMetadata.getThresholds().get("column_value_size");
        Thresholds t2 = newMetadata.getThresholds().get("collection_size");

        assertThat(t1).isNotNull();
        assertThat(t2).isNotNull();

        assertEquals(10, t1.warnThreshold);
        assertEquals(30, t1.failThreshold);

        assertEquals(100, t2.warnThreshold);
        assertEquals(150, t2.failThreshold);
    }

    @Test
    public void testDiffingCustoms()
    {
        GuardrailsMetadata oldMetadata = new GuardrailsMetadata(EMPTY);
        assertEquals(0, oldMetadata.getCustom().size());

        CustomGuardrailConfig defaultConfig = CassandraPasswordValidatorConfiguration.parse(new CustomGuardrailConfig()).asCustomGuardrailConfig();
        assertEquals(5, defaultConfig.get(ILLEGAL_SEQUENCE_LENGTH_KEY));
        GuardrailsMetadata newMetadata = oldMetadata.with(new Custom("password", defaultConfig));

        ImmutableMap<String, CustomGuardrailConfig> diff = oldMetadata.diffCustom(newMetadata);
        assertNotNull(diff);

        oldMetadata = newMetadata;

        CustomGuardrailConfig newConfig = new CustomGuardrailConfig(defaultConfig);
        newConfig.put(ILLEGAL_SEQUENCE_LENGTH_KEY, 10);

        newMetadata = oldMetadata.with(new Custom("password", newConfig));
        diff = oldMetadata.diffCustom(newMetadata);

        CustomGuardrailConfig diffedConfig = diff.get("password");
        assertNotNull(diffedConfig);
        assertEquals(10, diffedConfig.get(ILLEGAL_SEQUENCE_LENGTH_KEY));

        assertEquals(1, newMetadata.getCustom().size());
    }

    @Test
    public void testDiffingValues()
    {
        GuardrailsMetadata oldMetadata = new GuardrailsMetadata(EMPTY);
        assertEquals(0, oldMetadata.getValues().size());
        GuardrailsMetadata newMetadata = oldMetadata.with(new Values("read_consistency_levels",
                                                                     null, // warned
                                                                     null, // disallowed
                                                                     null));// ignored

        Map<String, ValuesHolder> diff = oldMetadata.diffValues(newMetadata);
        ValuesHolder valueHolder = diff.get("read_consistency_levels");
        assertNull(valueHolder);

        oldMetadata = newMetadata;

        newMetadata = oldMetadata.with(new Values("read_consistency_levels",
                                                  Set.of(), // warned
                                                  null, // disallowed
                                                  null));// ignored

        diff = oldMetadata.diffValues(newMetadata);
        valueHolder = diff.get("read_consistency_levels");
        assertNotNull(valueHolder);
        assertNull(valueHolder.disallowed);
        assertNull(valueHolder.ignored);
        assertNotNull(valueHolder.warned);
        assertThat(valueHolder.warned).isEmpty();

        oldMetadata = newMetadata;

        newMetadata = oldMetadata.with(new Values("read_consistency_levels",
                                                  Set.of(), // warned
                                                  null, // disallowed
                                                  null));// ignored

        diff = oldMetadata.diffValues(newMetadata);
        valueHolder = diff.get("read_consistency_levels");
        assertNull(valueHolder); // here it is null again because we are diffing exactly same Values

        oldMetadata = newMetadata;

        newMetadata = oldMetadata.with(new Values("read_consistency_levels",
                                                  Set.of(), // warned
                                                  Set.of(EACH_QUORUM.name()), // disallowed
                                                  null));// ignored

        diff = oldMetadata.diffValues(newMetadata);
        valueHolder = diff.get("read_consistency_levels");
        assertNotNull(valueHolder);
        assertNull(valueHolder.warned); // warned is null because now and in prev is empty
        assertThat(valueHolder.disallowed).hasSameElementsAs(Set.of(EACH_QUORUM.name()));
        assertNull(valueHolder.ignored);

        oldMetadata = newMetadata;

        newMetadata = oldMetadata.with(new Values("read_consistency_levels",
                                                  Set.of(QUORUM.name(), THREE.name()), // warned
                                                  Set.of(EACH_QUORUM.name()), // disallowed
                                                  null));// ignored

        diff = oldMetadata.diffValues(newMetadata);
        valueHolder = diff.get("read_consistency_levels");
        assertNotNull(valueHolder);
        assertThat(valueHolder.warned).hasSameElementsAs(Set.of(QUORUM.name(), THREE.name()));
        assertNull(valueHolder.disallowed);
        assertNull(valueHolder.ignored);

        oldMetadata = newMetadata;

        newMetadata = oldMetadata.with(new Values("read_consistency_levels",
                                                  Set.of(QUORUM.name()), // warned
                                                  Set.of(EACH_QUORUM.name(), ONE.name()), // disallowed
                                                  Set.of())); // ignored

        diff = oldMetadata.diffValues(newMetadata);
        valueHolder = diff.get("read_consistency_levels");
        assertNotNull(valueHolder);
        assertThat(valueHolder.warned).hasSameElementsAs(Set.of(QUORUM.name()));
        assertThat(valueHolder.disallowed).hasSameElementsAs(Set.of(EACH_QUORUM.name(), ONE.name()));
        assertThat(valueHolder.ignored).isEmpty();

        oldMetadata = newMetadata;

        newMetadata = oldMetadata.with(new Values("read_consistency_levels",
                                                  Set.of(QUORUM.name()), // warned
                                                  Set.of(EACH_QUORUM.name(), ONE.name()), // disallowed
                                                  Set.of())); // ignored

        diff = oldMetadata.diffValues(newMetadata);
        valueHolder = diff.get("read_consistency_levels");
        assertNull(valueHolder);

        assertEquals(1, newMetadata.getValues().size());

        newMetadata = oldMetadata.with(new Values("read_consistency_levels",
                                                  Set.of(QUORUM.name()), // warned
                                                  Set.of(TWO.name()), // disallowed
                                                  Set.of())) // ignored
                                 .with(new Values("write_consistency_levels",
                                                  Set.of(QUORUM.name()), // warned
                                                  Set.of(EACH_QUORUM.name(), ONE.name()), // disallowed
                                                  Set.of()));

        assertEquals(2, newMetadata.getValues().size());
    }
}
