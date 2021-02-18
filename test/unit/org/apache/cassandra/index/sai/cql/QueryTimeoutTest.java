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
package org.apache.cassandra.index.sai.cql;

import javax.management.ObjectName;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.datastax.driver.core.exceptions.ReadTimeoutException;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.disk.PostingListRangeIterator;
import org.apache.cassandra.index.sai.metrics.TableQueryMetrics;
import org.apache.cassandra.inject.Injection;
import org.apache.cassandra.inject.Injections;

import static org.apache.cassandra.inject.InvokePointBuilder.newInvokePoint;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class QueryTimeoutTest extends SAITester
{
    private static final int TIMEOUT = 5000;
    private static final int DELAY = TIMEOUT + (TIMEOUT / 2);

    private ObjectName queryCountName, queryTimeoutsName;

    @Before
    public void setup() throws Throwable
    {
        requireNetwork();

        startJMXServer();

        createMBeanServerConnection();

        DatabaseDescriptor.setRangeRpcTimeout(TIMEOUT);
        DatabaseDescriptor.setReadRpcTimeout(TIMEOUT);

        createTable(CREATE_TABLE_TEMPLATE);
        createIndex(String.format(CREATE_INDEX_TEMPLATE, "v1"));
        createIndex(String.format(CREATE_INDEX_TEMPLATE, "v2"));

        if (execute("SELECT * FROM %s").size() > 0)
        {
            return;
        }

        for (int i = 0; i < 100; ++i)
        {
            execute("INSERT INTO %s(id1,v1,v2) VALUES (?, ?, ?)", i, i, Integer.toString(i % 5));
        }
        flush();

        execute("SELECT * FROM %s WHERE v1 >= 0 AND v1 < 10000");
        execute("SELECT * FROM %s WHERE v2 = '0'");

        queryCountName = objectNameNoIndex("TotalQueriesCompleted", CQLTester.KEYSPACE, currentTable(), TableQueryMetrics.TABLE_QUERY_METRIC_TYPE);
        queryTimeoutsName = objectNameNoIndex("TotalQueryTimeouts", CQLTester.KEYSPACE, currentTable(), TableQueryMetrics.TABLE_QUERY_METRIC_TYPE);
    }

    @After
    public void removeInjections() throws Exception
    {
        Injections.deleteAll();
    }

    @Test
    public void delayDuringKDTreeIntersectionShouldProvokeTimeoutInReader() throws Throwable
    {
        Injection kdtree_intersection_delay = Injections.newPause("kdtree_intersection_delay", DELAY)
                                                        .add(newInvokePoint().onClass("org.apache.cassandra.index.sai.disk.v1.BKDReader$Intersection")
                                                                                   .onMethod("collectPostingLists")
                                                                                   .at("INVOKE QueryContext.checkpoint"))

                                                        .build();

        Injections.inject(kdtree_intersection_delay);

        assertThatThrownBy(() -> executeNet("SELECT * FROM %s WHERE v1 >= 0 AND v1 < 10000")).isInstanceOf(ReadTimeoutException.class);

        waitForEquals(queryCountName, queryTimeoutsName);
    }

    @Test
    public void delayDuringTermsReaderMatchShouldProvokeTimeoutInReader() throws Throwable
    {
        Injection terms_match_delay = Injections.newPause("terms_match_delay", DELAY)
                                                .add(newInvokePoint().onClass("org.apache.cassandra.index.sai.disk.v1.TermsReader$TermQuery")
                                                                           .onMethod("execute")
                                                                           .at("INVOKE QueryContext.checkpoint"))
                                                .build();

        Injections.inject(terms_match_delay);

        assertThatThrownBy(() -> executeNet("SELECT * FROM %s WHERE v2 = '1'")).isInstanceOf(ReadTimeoutException.class);

        waitForEquals(queryCountName, queryTimeoutsName);
    }

    @Test
    public void delayDuringTokenLookupShouldProvokeTimeoutInRangeIterator() throws Throwable
    {
        Injection token_lookup_delay = Injections.newPause("token_lookup_delay", DELAY)
                                                 .add(newInvokePoint().onClass(PostingListRangeIterator.class)
                                                                            .onMethod("computeNext")
                                                                            .at("INVOKE QueryContext.checkpoint"))
                                                 .build();

        Injections.inject(token_lookup_delay);

        assertThatThrownBy(() -> executeNet("SELECT * FROM %s WHERE v2 = '1'")).isInstanceOf(ReadTimeoutException.class);

        waitForEquals(queryCountName, queryTimeoutsName);
    }
}
