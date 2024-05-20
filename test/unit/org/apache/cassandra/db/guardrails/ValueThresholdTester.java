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

package org.apache.cassandra.db.guardrails;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.function.ToLongFunction;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.transport.messages.ResultMessage;

import static java.lang.String.format;
import static java.nio.ByteBuffer.allocate;

public abstract class ValueThresholdTester extends ThresholdTester
{
    protected ValueThresholdTester(String warnThreshold,
                                   String failThreshold,
                                   Threshold threshold,
                                   TriConsumer<Guardrails, String, String> setter,
                                   Function<Guardrails, String> warnGetter,
                                   Function<Guardrails, String> failGetter,
                                   Function<Long, String> stringFormatter,
                                   ToLongFunction<String> stringParser)
    {
        super(warnThreshold,
              failThreshold,
              threshold,
              setter,
              warnGetter,
              failGetter,
              stringFormatter,
              stringParser);
    }

    protected abstract int warnThreshold();

    protected abstract int failThreshold();

    /**
     * Tests that the max column size guardrail threshold is not applied for the specified 1-placeholder CQL query.
     *
     * @param query a CQL modification statement with exactly one placeholder
     */
    protected void testNoThreshold(String query) throws Throwable
    {
        assertValid(query, allocate(1));

        assertValid(query, allocate(warnThreshold()));
        assertValid(query, allocate(warnThreshold() + 1));

        assertValid(query, allocate(failThreshold()));
        assertValid(query, allocate(failThreshold() + 1));
    }

    /**
     * Tests that the max column size guardrail threshold is not applied for the specified 2-placeholder CQL query.
     *
     * @param query a CQL modification statement with exactly two placeholders
     */
    protected void testNoThreshold2(String query) throws Throwable
    {
        assertValid(query, allocate(1), allocate(1));

        assertValid(query, allocate(warnThreshold()), allocate(1));
        assertValid(query, allocate(1), allocate(warnThreshold()));
        assertValid(query, allocate((warnThreshold())), allocate((warnThreshold())));
        assertValid(query, allocate(warnThreshold() + 1), allocate(1));
        assertValid(query, allocate(1), allocate(warnThreshold() + 1));

        assertValid(query, allocate(failThreshold()), allocate(1));
        assertValid(query, allocate(1), allocate(failThreshold()));
        assertValid(query, allocate((failThreshold())), allocate((failThreshold())));
        assertValid(query, allocate(failThreshold() + 1), allocate(1));
        assertValid(query, allocate(1), allocate(failThreshold() + 1));
    }

    /**
     * Tests that the max column size guardrail threshold is applied for the specified 1-placeholder CQL query.
     *
     * @param column the name of the column referenced by the query placeholder
     * @param query  a CQL query with exactly one placeholder
     */
    protected void testThreshold(String column, String query) throws Throwable
    {
        testThreshold(column, query, 0);
    }

    /**
     * Tests that the max column size guardrail threshold is applied for the specified 1-placeholder CQL query.
     *
     * @param column             the name of the column referenced by the query placeholder
     * @param query              a CQL query with exactly one placeholder
     * @param serializationBytes the extra bytes added to the placeholder value by its wrapping column type serializer
     */
    protected void testThreshold(String column, String query, int serializationBytes) throws Throwable
    {
        int warn = warnThreshold() - serializationBytes;
        int fail = failThreshold() - serializationBytes;

        assertValid(query, allocate(0));
        assertValid(query, allocate(warn));
        assertWarns(column, query, allocate(warn + 1));
        assertFails(column, query, allocate(fail + 1));
    }

    /**
     * Tests that the max column size guardrail threshold is applied for the specified 2-placeholder CQL query.
     *
     * @param column the name of the column referenced by the placeholders
     * @param query  a CQL query with exactly two placeholders
     */
    protected void testThreshold2(String column, String query) throws Throwable
    {
        testThreshold2(column, query, 0);
    }

    /**
     * Tests that the max column size guardrail threshold is applied for the specified 2-placeholder query.
     *
     * @param column             the name of the column referenced by the placeholders
     * @param query              a CQL query with exactly two placeholders
     * @param serializationBytes the extra bytes added to the size of the placeholder value by their wrapping serializer
     */
    protected void testThreshold2(String column, String query, int serializationBytes) throws Throwable
    {
        int warn = warnThreshold() - serializationBytes;
        int fail = failThreshold() - serializationBytes;

        assertValid(query, allocate(0), allocate(0));
        assertValid(query, allocate(warn), allocate(0));
        assertValid(query, allocate(0), allocate(warn));
        assertValid(query, allocate(warn / 2), allocate(warn / 2));

        assertWarns(column, query, allocate(warn + 1), allocate(0));
        assertWarns(column, query, allocate(0), allocate(warn + 1));

        assertFails(column, query, allocate(fail + 1), allocate(0));
        assertFails(column, query, allocate(0), allocate(fail + 1));
    }

    protected void testCollection(String column, String query, Function<ByteBuffer[], ByteBuffer> collectionBuilder) throws Throwable
    {
        assertValid(query, collectionBuilder, allocate(1));
        assertValid(query, collectionBuilder, allocate(1), allocate(1));
        assertValid(query, collectionBuilder, allocate(warnThreshold()));
        assertValid(query, collectionBuilder, allocate(warnThreshold()), allocate(1));
        assertValid(query, collectionBuilder, allocate(1), allocate(warnThreshold()));
        assertValid(query, collectionBuilder, allocate(warnThreshold()), allocate(warnThreshold()));

        assertWarns(column, query, collectionBuilder, allocate(warnThreshold() + 1));
        assertWarns(column, query, collectionBuilder, allocate(warnThreshold() + 1), allocate(1));
        assertWarns(column, query, collectionBuilder, allocate(1), allocate(warnThreshold() + 1));

        assertFails(column, query, collectionBuilder, allocate(failThreshold() + 1));
        assertFails(column, query, collectionBuilder, allocate(failThreshold() + 1), allocate(1));
        assertFails(column, query, collectionBuilder, allocate(1), allocate(failThreshold() + 1));
    }

    protected void testFrozenCollection(String column, String query, Function<ByteBuffer[], ByteBuffer> collectionBuilder) throws Throwable
    {
        assertValid(query, collectionBuilder, allocate(1));
        assertValid(query, collectionBuilder, allocate(warnThreshold() - 8));
        assertValid(query, collectionBuilder, allocate((warnThreshold() - 12) / 2), allocate((warnThreshold() - 12) / 2));

        assertWarns(column, query, collectionBuilder, allocate(warnThreshold() - 7));
        assertWarns(column, query, collectionBuilder, allocate(warnThreshold() - 12), allocate(1));

        assertFails(column, query, collectionBuilder, allocate(failThreshold() - 7));
        assertFails(column, query, collectionBuilder, allocate(failThreshold() - 12), allocate(1));
    }

    protected void testMap(String column, String query) throws Throwable
    {
        assertValid(query, this::map, allocate(1), allocate(1));
        assertValid(query, this::map, allocate(warnThreshold()), allocate(1));
        assertValid(query, this::map, allocate(1), allocate(warnThreshold()));
        assertValid(query, this::map, allocate(warnThreshold()), allocate(warnThreshold()));

        assertWarns(column, query, this::map, allocate(1), allocate(warnThreshold() + 1));
        assertWarns(column, query, this::map, allocate(warnThreshold() + 1), allocate(1));

        assertFails(column, query, this::map, allocate(failThreshold() + 1), allocate(1));
        assertFails(column, query, this::map, allocate(1), allocate(failThreshold() + 1));
        assertFails(column, query, this::map, allocate(failThreshold() + 1), allocate(failThreshold() + 1));
    }

    protected void assertValid(String query, ByteBuffer... values) throws Throwable
    {
        assertValid(() -> execute(query, values));
    }

    protected void assertValid(String query, Function<ByteBuffer[], ByteBuffer> collectionBuilder, ByteBuffer... values) throws Throwable
    {
        assertValid(() -> execute(query, collectionBuilder.apply(values)));
    }

    protected void assertWarns(String column, String query, Function<ByteBuffer[], ByteBuffer> collectionBuilder, ByteBuffer... values) throws Throwable
    {
        assertWarns(column, query, collectionBuilder.apply(values));
    }

    protected void assertWarns(String column, String query, ByteBuffer... values) throws Throwable
    {
        String errorMessage = format("Value of column '%s' has size %s, this exceeds the warning threshold of %s.",
                                     column, warnThreshold() + 1, warnThreshold());
        assertWarns(() -> execute(query, values), errorMessage);
    }

    protected void assertFails(String column, String query, Function<ByteBuffer[], ByteBuffer> collectionBuilder, ByteBuffer... values) throws Throwable
    {
        assertFails(column, query, collectionBuilder.apply(values));
    }

    protected void assertFails(String column, String query, ByteBuffer... values) throws Throwable
    {
        String errorMessage = format("Value of column '%s' has size %s, this exceeds the failure threshold of %s.",
                                     column, failThreshold() + 1, failThreshold());
        assertFails(() -> execute(query, values), errorMessage);
    }

    protected ResultMessage execute(String query, ByteBuffer... values)
    {
        return execute(userClientState, query, Arrays.asList(values));
    }

    protected ByteBuffer set(ByteBuffer... values)
    {
        return SetType.getInstance(BytesType.instance, true).decompose(ImmutableSet.copyOf(values));
    }

    protected ByteBuffer list(ByteBuffer... values)
    {
        return ListType.getInstance(BytesType.instance, true).decompose(ImmutableList.copyOf(values));
    }

    protected ByteBuffer map(ByteBuffer... values)
    {
        assert values.length % 2 == 0;

        int size = values.length / 2;
        Map<ByteBuffer, ByteBuffer> m = new LinkedHashMap<>(size);
        for (int i = 0; i < size; i++)
            m.put(values[2 * i], values[(2 * i) + 1]);

        return MapType.getInstance(BytesType.instance, BytesType.instance, true).decompose(m);
    }
}
