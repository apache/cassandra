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

package org.apache.cassandra.tcm;

import java.io.IOException;
import java.util.Random;

import com.google.common.collect.Iterables;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.tcm.ClusterMetadata.Transformer.Transformed;
import org.apache.cassandra.tcm.extensions.EpochValue;
import org.apache.cassandra.tcm.extensions.ExtensionKey;
import org.apache.cassandra.tcm.extensions.ExtensionValue;
import org.apache.cassandra.tcm.extensions.IntValue;
import org.apache.cassandra.tcm.extensions.StringValue;
import org.apache.cassandra.tcm.serialization.Version;

import static org.apache.cassandra.tcm.sequences.SequencesUtils.epoch;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class ClusterMetadataTransformationTest
{
    long seed = System.nanoTime();
    Random random = new Random(seed);

    @BeforeClass
    public static void setupClass()
    {
        DatabaseDescriptor.setPartitionerUnsafe(Murmur3Partitioner.instance);
    }

    @Test
    public void testModifyExtendedMetadata()
    {
        long seed = System.nanoTime();
        Random r = new Random(seed);
        ClusterMetadata metadata = new ClusterMetadata(Murmur3Partitioner.instance);
        ExtensionKey<Epoch, EpochValue> testKey = new ExtensionKey<>("foo.bar", EpochValue.class);
        Epoch start = epoch(r);

        // write initial value with extension key
        Transformed transformed = metadata.transformer().with(testKey, EpochValue.create(start)).build();
        assertModifications(transformed, testKey);
        ExtensionValue<?> stored = transformed.metadata.extensions.get(testKey);
        Epoch actual = (Epoch) stored.getValue();
        assertEquals(start, actual);

        // overwrite initial value
        Epoch updated = start.nextEpoch();
        transformed = transformed.metadata.transformer().with(testKey, EpochValue.create(updated)).build();
        assertModifications(transformed, testKey);
        stored = transformed.metadata.extensions.get(testKey);
        actual = (Epoch) stored.getValue();
        assertEquals(updated, actual);

        // don't overwrite using withIfAbsent
        Epoch updatedAgain = updated.nextEpoch();
        transformed = transformed.metadata.transformer().withIfAbsent(testKey, EpochValue.create(updatedAgain)).build();
        assertModifications(transformed);
        stored = transformed.metadata.extensions.get(testKey);
        actual = (Epoch) stored.getValue();
        assertEquals(updated, actual);

        // remove value
        transformed = transformed.metadata.transformer().without(testKey).build();
        assertModifications(transformed, testKey);
        assertNull(transformed.metadata.extensions.get(testKey));

        // now write usint withIfAbsent
        transformed = transformed.metadata.transformer().withIfAbsent(testKey, EpochValue.create(updatedAgain)).build();
        assertModifications(transformed, testKey);
        stored = transformed.metadata.extensions.get(testKey);
        actual = (Epoch) stored.getValue();
        assertEquals(updatedAgain, actual);
    }

    @Test
    public void testRoundTripMetadataExtensions() throws IOException
    {
        ClusterMetadata metadata = new ClusterMetadata(Murmur3Partitioner.instance);
        ExtensionKey<Epoch, EpochValue> epochKey = new ExtensionKey<>("test.epoch", EpochValue.class);
        EpochValue e = EpochValue.create(epoch(random));

        ExtensionKey<Integer, IntValue> intKey = new ExtensionKey<>("test.int", IntValue.class);
        IntValue i = IntValue.create(random.nextInt());

        ExtensionKey<String, StringValue> stringKey = new ExtensionKey<>("test.string", StringValue.class);
        StringValue s = StringValue.create("" + random.nextInt());

        Transformed transformed = metadata.transformer()
                                          .with(epochKey, e)
                                          .with(intKey, i)
                                          .with(stringKey, s)
                                          .build();
        assertModifications(transformed, epochKey, intKey, stringKey);

        DataOutputBuffer out = new DataOutputBuffer();
        ClusterMetadata.serializer.serialize(transformed.metadata, out, Version.V0);
        DataInputBuffer in = new DataInputBuffer(out.buffer(), false);
        ClusterMetadata newMeta = ClusterMetadata.serializer.deserialize(in, Version.V0);

        assertEquals(e.getValue(), newMeta.extensions.get(epochKey).getValue());
        assertEquals(i.getValue(), newMeta.extensions.get(intKey).getValue());
        assertEquals(s.getValue(), newMeta.extensions.get(stringKey).getValue());
    }

    private static void assertModifications(Transformed transformed, MetadataKey... expected)
    {
        assertEquals(expected.length, transformed.modifiedKeys.size());
        for (MetadataKey key : expected)
            assertTrue(transformed.modifiedKeys.contains(key));

        // anything modified by in this transformation, and therefore included in the modified keys,
        // should have the same epoch as the CM itself. Anything not modified now must have a strictly
        // earlier epoch
        for (MetadataKey key : Iterables.concat(MetadataKeys.CORE_METADATA, transformed.metadata.extensions.keySet()))
        {
            MetadataValue<?> value = valueFor(key, transformed.metadata);
            if (transformed.modifiedKeys.contains(key))
                assertEquals(transformed.metadata.epoch, value.lastModified());
            else
                assertTrue(transformed.metadata.epoch.isAfter(value.lastModified()));
        }
    }

    private static MetadataValue<?> valueFor(MetadataKey key, ClusterMetadata metadata)
    {
        if (!MetadataKeys.CORE_METADATA.contains(key))
        {
            assert key instanceof ExtensionKey<?,?>;
            return metadata.extensions.get((ExtensionKey<?, ?>)key);
        }

        throw new IllegalArgumentException("Unknown metadata key " + key);
    }

}
