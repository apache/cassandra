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

import com.datastax.driver.core.exceptions.ReadFailureException;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.SSTableContext;
import org.apache.cassandra.index.sai.disk.v1.PostingsReader;
import org.apache.cassandra.index.sai.disk.v1.TermsReader;
import org.apache.cassandra.inject.Injection;
import org.apache.cassandra.inject.Injections;
import org.apache.cassandra.utils.Throwables;

import static org.apache.cassandra.inject.ActionBuilder.newActionBuilder;
import static org.apache.cassandra.inject.Expression.quote;
import static org.apache.cassandra.inject.InvokePointBuilder.newInvokePoint;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class SingleNodeQueryFailureTest extends SAITester
{
    private static final String CREATE_TABLE_TEMPLATE = "CREATE TABLE %s (id text PRIMARY KEY, v1 int, v2 text) WITH compaction = {'class' : 'SizeTieredCompactionStrategy', 'enabled' : false }";

    @Before
    public void setup() throws Throwable
    {
        requireNetwork();
    }

    @After
    public void teardown()
    {
        Injections.deleteAll();
    }

    @Test
    public void testFailedRangeIteratorOnMultiIndexesQuery() throws Throwable
    {
        testFailedMultiIndexesQuery("range_iterator", PostingListRangeIterator.class, "getNextSegmentRowId");
    }

    @Test
    public void testFailedTermsReaderOnMultiIndexesQuery() throws Throwable
    {
        testFailedMultiIndexesQuery("terms_reader", TermsReader.TermQuery.class, "lookupTermDictionary");
    }

    @Test
    public void testFailedBkdReaderOnMultiIndexesQuery() throws Throwable
    {
        testFailedMultiIndexesQuery("bkd_reader", PostingsReader.class, "<init>");
    }

    @Test
    public void testFailedKeyFetcherOnMultiIndexesQuery() throws Throwable
    {
        testFailedMultiIndexesQuery("key_fetcher", SSTableContext.DecoratedKeyFetcher.class, "apply");
    }

    @Test
    public void testFailedKeyReaderOnMultiIndexesQuery() throws Throwable
    {
        testFailedMultiIndexesQuery("key_reader", SSTableContext.DecoratedKeyFetcher.class, "createReader");
    }

    private void testFailedMultiIndexesQuery(String name, Class<?> targetClass, String targetMethod) throws Throwable
    {
        String table = "test_mixed_index_query_" + name;

        Injection injection = Injections.newCustom(name)
                                        .add(newInvokePoint().onClass(targetClass).onMethod(targetMethod))
                                        .add(newActionBuilder().actions().doThrow(RuntimeException.class, quote("Injected failure!")))
                                        .build();

        createTable(CREATE_TABLE_TEMPLATE);
        createIndex(String.format(CREATE_INDEX_TEMPLATE, "v1"));
        createIndex(String.format(CREATE_INDEX_TEMPLATE, "v2"));
        waitForIndexQueryable();

        execute("INSERT INTO %s (id, v1, v2) VALUES ('1', 0, '0')");
        flush();
        execute("INSERT INTO %s (id, v1, v2) VALUES ('2', 1, '1')");
        flush();
        execute("INSERT INTO %s (id, v1, v2) VALUES ('3', 2, '2')");
        flush();

        try
        {
            Injections.inject(injection);

            assertThatThrownBy(() -> executeNet("SELECT id FROM %s WHERE v1 < 1 and v2 = '0'"))
                    .isInstanceOf(ReadFailureException.class);

            assertThatThrownBy(() -> executeNet("SELECT id FROM %s WHERE v1 >= 1 and v2 = '1'"))
                    .isInstanceOf(ReadFailureException.class);

            assertThatThrownBy(() -> executeNet("SELECT id FROM %s WHERE v1 >= 2 and v2 = '2'"))
                    .isInstanceOf(ReadFailureException.class);
        }
        catch (Exception e)
        {
            throw Throwables.unchecked(e);
        }
        finally
        {
            injection.disable();
        }

        Assert.assertEquals(3, executeNet("SELECT id FROM %s WHERE v1 >= 0").all().size());
        Assert.assertEquals(1, executeNet("SELECT id FROM %s WHERE v2 = '0'").all().size());
        Assert.assertEquals(1, executeNet("SELECT id FROM %s WHERE v2 = '1'").all().size());
        Assert.assertEquals(1, executeNet("SELECT id FROM %s WHERE v2 = '2'").all().size());
    }
}
