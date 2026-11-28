package org.apache.cassandra.schema;

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

import org.assertj.core.api.Assertions;
import org.junit.Test;

import accord.utils.Gen;
import accord.utils.Gens;
import accord.utils.LazyToString;
import accord.utils.ReflectionUtils;

import org.apache.cassandra.io.Serializers;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.utils.CassandraGenerators;
import org.apache.cassandra.utils.Generators;

import static accord.utils.Property.qt;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TableIdTest
{
    @Test
    public void serialize()
    {
        @SuppressWarnings({ "resource", "IOResourceOpenedButNotSafelyClosed" }) DataOutputBuffer output = new DataOutputBuffer();
        qt().forAll(Generators.toGen(CassandraGenerators.TABLE_ID_GEN)).check(input -> {
            output.clear();
            input.serialize(output);
            Assertions.assertThat(output.getLength()).describedAs("The serialized size and bytes written do not match").isEqualTo(input.serializedSize());

            DataInputBuffer in = new DataInputBuffer(output.unsafeGetBufferAndFlip(), false);
            TableId read = TableId.deserialize(in);
            Assertions.assertThat(read).describedAs("The deserialized output does not match the serialized input; difference %s", new LazyToString(() -> ReflectionUtils.recursiveEquals(read, input).toString())).isEqualTo(input);
        });
    }

    @Test
    public void serializeCompact()
    {
        @SuppressWarnings({ "resource", "IOResourceOpenedButNotSafelyClosed" }) DataOutputBuffer output = new DataOutputBuffer();
        qt().forAll(Generators.toGen(CassandraGenerators.TABLE_ID_GEN)).check(input -> {
            output.clear();
            input.serializeCompact(output);
            Assertions.assertThat(output.getLength()).describedAs("The serialized size and bytes written do not match").isEqualTo(input.serializedCompactSize());

            DataInputBuffer in = new DataInputBuffer(output.unsafeGetBufferAndFlip(), false);
            TableId read = TableId.deserializeCompact(in);
            Assertions.assertThat(read).describedAs("The deserialized output does not match the serialized input; difference %s", new LazyToString(() -> ReflectionUtils.recursiveEquals(read, input).toString())).isEqualTo(input);
        });
    }

    @Test
    public void serializeCompactComparable()
    {
        @SuppressWarnings({ "resource", "IOResourceOpenedButNotSafelyClosed" }) DataOutputBuffer output = new DataOutputBuffer();
        qt().forAll(Generators.toGen(CassandraGenerators.TABLE_ID_GEN)).check(input -> {
            output.clear();
            input.serializeCompactComparable(output);
            Assertions.assertThat(output.getLength()).describedAs("The serialized size and bytes written do not match").isEqualTo(input.serializedCompactComparableSize());

            DataInputBuffer in = new DataInputBuffer(output.unsafeGetBufferAndFlip(), false);
            TableId read = TableId.deserializeCompactComparable(in);
            Assertions.assertThat(read).describedAs("The deserialized output does not match the serialized input; difference %s", new LazyToString(() -> ReflectionUtils.recursiveEquals(read, input).toString())).isEqualTo(input);
        });
    }

    @Test
    public void serializeCompactComparableV2()
    {
        @SuppressWarnings({ "resource", "IOResourceOpenedButNotSafelyClosed" }) DataOutputBuffer output = new DataOutputBuffer();
        qt().forAll(Generators.toGen(CassandraGenerators.TABLE_ID_GEN))
            .check(input -> Serializers.testSerde(output, TableId.compactComparableSerializer, input));
    }

    /**
     * Round-trip: fromString(tableId.toString()) must recover the original TableId.
     * This exercises both the "tid:" path (MAGIC msb, uses Hex.parseLong) and the
     * UUID path (non-MAGIC msb, uses UUID.fromString).
     */
    @Test
    public void fromStringRoundTrip()
    {
        qt().forAll(Generators.toGen(CassandraGenerators.TABLE_ID_GEN)).check(input -> {
            String str = input.toString();
            TableId recovered = TableId.fromString(str);
            assertThat(recovered)
                .describedAs("fromString(toString()) must recover the original TableId for: %s", str)
                .isEqualTo(input);
        });
    }

    /**
     * fromString must handle uppercase hex characters in the "tid:" format.
     * Long.toHexString produces lowercase, but callers may pass uppercase.
     */
    @Test
    public void fromStringHandlesUppercaseHex()
    {
        qt().forAll(Gens.longs().all()).check(lsb -> {
            String lowercase = "tid:" + Long.toHexString(lsb);
            String uppercase = "tid:" + Long.toHexString(lsb).toUpperCase();

            TableId fromLower = TableId.fromString(lowercase);
            TableId fromUpper = TableId.fromString(uppercase);
            assertThat(fromUpper)
                .describedAs("fromString must handle uppercase hex: %s vs %s", uppercase, lowercase)
                .isEqualTo(fromLower);
        });
    }
 }
