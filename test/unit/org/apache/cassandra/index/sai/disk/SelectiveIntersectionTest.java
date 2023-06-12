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

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.datastax.driver.core.Session;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.disk.v1.postings.PostingsReader;
import org.apache.cassandra.inject.Injections;

import static org.apache.cassandra.inject.InvokePointBuilder.newInvokePoint;
import static org.junit.Assert.assertEquals;

public class SelectiveIntersectionTest extends SAITester
{
    private static final Injections.Counter intersectionFlowCounter = Injections.newCounter("IntersectionFlowCounter")
                                                                                .add(newInvokePoint().onClass("org.apache.cassandra.index.sai.iterators.KeyRangeIntersectionIterator").onMethod("<init>"))
                                                                                .build();

    private static final Injections.Counter postingsReaderOpenCounter = Injections.newCounter("PostingsReaderOpenCounter")
                                                                                  .add(newInvokePoint().onClass(PostingsReader.class).onMethod("<init>"))
                                                                                  .build();

    private static final Injections.Counter postingsReaderCloseCounter = Injections.newCounter("PostingsReaderCloseCounter")
                                                                                   .add(newInvokePoint().onClass(PostingsReader.class).onMethod("close"))
                                                                                   .build();


    @Before
    public void setup() throws Throwable
    {
        requireNetwork();

        Injections.inject(intersectionFlowCounter, postingsReaderOpenCounter, postingsReaderCloseCounter);

        setLimits(2);

        createTable("CREATE TABLE %s (pk int primary key, v1 text, v2 text, v3 text)");
        createIndex(String.format(CREATE_INDEX_TEMPLATE, "v1"));
        createIndex(String.format(CREATE_INDEX_TEMPLATE, "v2"));
        createIndex(String.format(CREATE_INDEX_TEMPLATE, "v3"));

        for (int i = 0; i < 100; ++i)
        {
            execute("INSERT INTO %s(pk,v1,v2,v3) VALUES (?, ?, ?, ?)", i, Integer.toString(i), Integer.toString(i / 20), Integer.toString(i % 10));
        }
        flush();
    }

    @After
    public void resetCounters()
    {
        intersectionFlowCounter.reset();
        postingsReaderOpenCounter.reset();
        postingsReaderCloseCounter.reset();
    }

    @Test
    public void queryBelowSelectiveLimitUsesDeferredFlows() throws Throwable
    {
        assertEquals(1, execute("SELECT * FROM %s WHERE v1 = '0'").size());
        Assert.assertEquals(0, intersectionFlowCounter.get());

        Assert.assertEquals(postingsReaderOpenCounter.get(), postingsReaderCloseCounter.get());
    }

    @Test
    public void queryAtSelectiveLimitUsesDeferredFlows() throws Throwable
    {
        assertEquals(1, execute("SELECT * FROM %s WHERE v1 = '20' AND v2 = '1'").size());
        Assert.assertEquals(1, intersectionFlowCounter.get());

        Assert.assertEquals(postingsReaderOpenCounter.get(), postingsReaderCloseCounter.get());
    }

    @Test
    public void queryAboveSelectiveLimitUsesDirectFlows() throws Throwable
    {
        assertEquals(1, execute("SELECT * FROM %s WHERE v1 = '1' AND v2 = '0' AND v3 = '1'").size());
        Assert.assertEquals(1, intersectionFlowCounter.get());

        Assert.assertEquals(postingsReaderOpenCounter.get(), postingsReaderCloseCounter.get());
    }

    @Test
    public void selectivityOfOneWillNotIntersect() throws Throwable
    {
        setLimits(1);

        assertEquals(1, execute("SELECT * FROM %s WHERE v1 = '1' AND v2 = '0' AND v3 = '1'").size());
        Assert.assertEquals(0, intersectionFlowCounter.get());

        Assert.assertEquals(postingsReaderOpenCounter.get(), postingsReaderCloseCounter.get());
    }

    @Test
    public void selectivityLimitOfZeroDisablesSelectivityTest() throws Throwable
    {
        setLimits(0);

        assertEquals(1, execute("SELECT * FROM %s WHERE v1 = '1' AND v2 = '0' AND v3 = '1'").size());
        Assert.assertEquals(1, intersectionFlowCounter.get());

        Assert.assertEquals(postingsReaderOpenCounter.get(), postingsReaderCloseCounter.get());
    }

    @Test
    public void tracingIsCorrectlyReported() throws Throwable
    {
        Session session = sessionNet();

        String trace = getSingleTraceStatement(session, "SELECT * FROM %s WHERE v1 = '1' AND v2 = '0' AND v3 = '1'", "Selecting");

        assertEquals("Selecting 2 indexes with cardinalities of 1, 10 out of 3 indexes", trace);

        setLimits(1);

        trace = getSingleTraceStatement(session, "SELECT * FROM %s WHERE v1 = '1' AND v2 = '0' AND v3 = '1'", "Selecting");

        assertEquals("Selecting 1 index with cardinality of 1 out of 3 indexes", trace);

        Assert.assertEquals(postingsReaderOpenCounter.get(), postingsReaderCloseCounter.get());
    }

    private static void setLimits(final int selectivityLimit)
    {
        CassandraRelevantProperties.SAI_INTERSECTION_CLAUSE_LIMIT.setString(Integer.toString(selectivityLimit));
    }
}
