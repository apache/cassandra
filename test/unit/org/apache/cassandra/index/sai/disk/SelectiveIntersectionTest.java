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
package org.apache.cassandra.index.sai.disk;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.datastax.driver.core.Session;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.disk.v1.PostingsReader;
import org.apache.cassandra.index.sai.utils.RangeIntersectionIterator;
import org.apache.cassandra.inject.Injections;

import static org.apache.cassandra.inject.InvokePointBuilder.newInvokePoint;
import static org.junit.Assert.assertEquals;

public class SelectiveIntersectionTest extends SAITester
{
    private static Injections.Counter INTERSECTION_FLOW_COUNTER = Injections.newCounter("IntersectionFlowCounter")
                                                                            .add(newInvokePoint().onClass("org.apache.cassandra.index.sai.utils.RangeIntersectionIterator$BounceIntersectionIterator").onMethod("<init>"))
                                                                            .build();

    private static Injections.Counter POSTINGS_READER_OPEN_COUNTER = Injections.newCounter("PostingsReaderOpenCounter")
                                                                               .add(newInvokePoint().onClass(PostingsReader.class).onMethod("<init>"))
                                                                               .build();

    private static Injections.Counter POSTINGS_READER_CLOSE_COUNTER = Injections.newCounter("PostingsReaderCloseCounter")
                                                                                .add(newInvokePoint().onClass(PostingsReader.class).onMethod("close"))
                                                                                .build();


    @Before
    public void setup() throws Throwable
    {
        requireNetwork();

        Injections.inject(INTERSECTION_FLOW_COUNTER, POSTINGS_READER_OPEN_COUNTER, POSTINGS_READER_CLOSE_COUNTER);

        setLimits(2);

        createTable("CREATE TABLE %s (pk int primary key, v1 int, v2 text, v3 text)");
        createIndex(String.format(CREATE_INDEX_TEMPLATE, "v1"));
        createIndex(String.format(CREATE_INDEX_TEMPLATE, "v2"));
        createIndex(String.format(CREATE_INDEX_TEMPLATE, "v3"));

        for (int i = 0; i < 100; ++i)
        {
            execute("INSERT INTO %s(pk,v1,v2,v3) VALUES (?, ?, ?, ?)", i, i, Integer.toString(i / 20), Integer.toString(i % 10));
        }
        flush();
    }

    @After
    public void resetCounters()
    {
        INTERSECTION_FLOW_COUNTER.reset();
        POSTINGS_READER_OPEN_COUNTER.reset();
        POSTINGS_READER_CLOSE_COUNTER.reset();
    }

    @Test
    public void queryBelowSelectiveLimitUsesDeferredFlows() throws Throwable
    {
        assertEquals(50, execute("SELECT * FROM %s WHERE v1 >= 0 AND v1 < 50").size());
        Assert.assertEquals(0, INTERSECTION_FLOW_COUNTER.get());

        Assert.assertEquals(POSTINGS_READER_OPEN_COUNTER.get(), POSTINGS_READER_CLOSE_COUNTER.get());
    }

    @Test
    public void queryAtSelectiveLimitUsesDeferredFlows() throws Throwable
    {
        assertEquals(20, execute("SELECT * FROM %s WHERE v1 >= 0 AND v1 < 50 AND v2 = '1'").size());
        Assert.assertEquals(1, INTERSECTION_FLOW_COUNTER.get());

        Assert.assertEquals(POSTINGS_READER_OPEN_COUNTER.get(), POSTINGS_READER_CLOSE_COUNTER.get());
    }

    @Test
    public void queryAboveSelectiveLimitUsesDirectFlows() throws Throwable
    {
        assertEquals(2, execute("SELECT * FROM %s WHERE v1 >= 0 AND v1 < 50 AND v2 = '1' AND v3 = '9'").size());
        Assert.assertEquals(1, INTERSECTION_FLOW_COUNTER.get());

        Assert.assertEquals(POSTINGS_READER_OPEN_COUNTER.get(), POSTINGS_READER_CLOSE_COUNTER.get());
    }

    @Test
    public void selectivityOfOneWillNotIntersect() throws Throwable
    {
        setLimits(1);

        assertEquals(2, execute("SELECT * FROM %s WHERE v1 >= 0 AND v1 < 50 AND v2 = '1' AND v3 = '9'").size());
        Assert.assertEquals(0, INTERSECTION_FLOW_COUNTER.get());

        Assert.assertEquals(POSTINGS_READER_OPEN_COUNTER.get(), POSTINGS_READER_CLOSE_COUNTER.get());
    }

    @Test
    public void selectivityLimitOfZeroDisablesSelectivityTest() throws Throwable
    {
        setLimits(0);

        assertEquals(2, execute("SELECT * FROM %s WHERE v1 >= 0 AND v1 < 50 AND v2 = '1' AND v3 = '9'").size());
        Assert.assertEquals(1, INTERSECTION_FLOW_COUNTER.get());

        Assert.assertEquals(POSTINGS_READER_OPEN_COUNTER.get(), POSTINGS_READER_CLOSE_COUNTER.get());
    }

    @Test
    public void tracingIsCorrectlyReported() throws Throwable
    {
        Session session = sessionNet();

        String trace = getSingleTraceStatement(session, "SELECT * FROM %s WHERE v1 >= 0 AND v1 < 50 AND v2 = '1' AND v3 = '9'", "Selecting");

        assertEquals("Selecting 2 indexes with cardinalities of 10, 20 out of 3 indexes", trace);

        setLimits(1);

        trace = getSingleTraceStatement(session, "SELECT * FROM %s WHERE v1 >= 0 AND v1 < 50 AND v2 = '1' AND v3 = '9'", "Selecting");

        assertEquals("Selecting 1 index with cardinality of 10 out of 3 indexes", trace);

        Assert.assertEquals(POSTINGS_READER_OPEN_COUNTER.get(), POSTINGS_READER_CLOSE_COUNTER.get());
    }

    private static void setLimits(final int selectivityLimit) throws Exception
    {
        Field selectivity = RangeIntersectionIterator.class.getDeclaredField("INTERSECTION_CLAUSE_LIMIT");
        selectivity.setAccessible(true);
        Field modifiersField = Field.class.getDeclaredField("modifiers");
        modifiersField.setAccessible(true);
        modifiersField.setInt(selectivity, selectivity.getModifiers() & ~Modifier.FINAL);
        selectivity.set(null, selectivityLimit);
    }
}
