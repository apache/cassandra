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

package org.apache.cassandra.io.sstable;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.mockito.Mockito;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(Parameterized.class)
public class SSTableUniqueIdentifierTest
{
    @Parameterized.Parameter(0)
    public SSTableUniqueIdentifier.Builder<? extends SSTableUniqueIdentifier> builder;

    @Parameterized.Parameter(1)
    public int defaultComparisonResult;

    @Parameterized.Parameters
    public static Collection<Object[]> parameters()
    {
        return Arrays.asList(new Object[]{ SequenceBasedSSTableUniqueIdentifier.Builder.instance, -1 },
                             new Object[]{ ULIDBasedSSTableUniqueIdentifier.Builder.instance, 1 });
    }

    @Test
    public void testSerializationToBytes()
    {
        Stream.generate(builder.generator(Stream.empty())).limit(100).forEachOrdered(id -> {
            try
            {
                ByteBuffer serialized = id.asBytes();
                SSTableUniqueIdentifier deserialized = builder.fromBytes(serialized);
                SSTableUniqueIdentifier fromFactory = SSTableUniqueIdentifierFactory.instance.fromBytes(serialized);
                assertThat(deserialized).isEqualTo(id);
                assertThat(fromFactory).isEqualTo(id);
            }
            catch (RuntimeException ex)
            {
                throw new RuntimeException("Failed for " + id, ex);
            }
        });
    }

    @Test
    public void testSerializationToString()
    {
        Stream.generate(builder.generator(Stream.empty())).limit(100).forEachOrdered(id -> {
            try
            {
                String serialized = id.asString();
                SSTableUniqueIdentifier deserialized = builder.fromString(serialized);
                SSTableUniqueIdentifier fromFactory = SSTableUniqueIdentifierFactory.instance.fromString(serialized);
                assertThat(deserialized).isEqualTo(id);
                assertThat(fromFactory).isEqualTo(id);
            }
            catch (RuntimeException ex)
            {
                throw new RuntimeException("Failed for " + id, ex);
            }
        });
    }

    @Test
    public void testComparison()
    {
        Supplier<? extends SSTableUniqueIdentifier> gen = builder.generator(Stream.empty());
        Stream.generate(gen).limit(100).forEachOrdered(id -> {
            try
            {
                assertThat(id.compareTo(Mockito.mock(SSTableUniqueIdentifier.class))).isEqualTo(defaultComparisonResult);
                SSTableUniqueIdentifier another = gen.get();
                assertThat(id.compareTo(another)).isEqualTo(-another.compareTo(id));
                assertThat(id).isNotEqualTo(another);
            }
            catch (RuntimeException ex)
            {
                throw new RuntimeException("Failed for " + id, ex);
            }
        });
    }
}