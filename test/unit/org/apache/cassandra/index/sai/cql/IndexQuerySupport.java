/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.cassandra.index.sai.cql;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.marshal.InetAddressType;
import org.apache.cassandra.db.marshal.SimpleDateType;
import org.apache.cassandra.db.marshal.TimeType;
import org.apache.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.plan.StorageAttachedIndexSearcher;
import org.apache.cassandra.inject.Injections;
import org.apache.cassandra.utils.Pair;
import org.hamcrest.Matchers;

import static org.apache.cassandra.index.sai.cql.DataModel.INET_COLUMN;
import static org.apache.cassandra.inject.InvokePointBuilder.newInvokePoint;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

/**
 * A CQL-based test framework for simulating queries across as much of the index state space as possible.
 *
 * This includes, but need not be limited to...
 *
 * 1.) ...queries on the same data as it migrates through the write path and storage engine.
 * 2.) ...queries across all supported native data types.
 * 3.) ...queries for all supported operators and value boundaries.
 * 4.) ...queries for varying write, update, delete, and TTL workloads.
 * 5.) ...queries across varying primary key and table structures.
 * 6.) ...queries across static, normal, and clustering column types.
 * 7.) ...queries across various paging and limit settings.
 */
@RunWith(Parameterized.class)
public abstract class IndexQuerySupport extends SAITester
{
    static List<BaseQuerySet> BASE_QUERY_SETS = ImmutableList.of(new BaseQuerySet(10, 5),
                                                                 new BaseQuerySet(10, 9),
                                                                 new BaseQuerySet(10, 10),
                                                                 new BaseQuerySet(10, Integer.MAX_VALUE),
                                                                 new BaseQuerySet(24, 10),
                                                                 new BaseQuerySet(24, 100),
                                                                 new BaseQuerySet(24, Integer.MAX_VALUE));

    static List<BaseQuerySet> COMPOSITE_PARTITION_QUERY_SETS = ImmutableList.of(new CompositePartitionQuerySet(10, 5),
                                                                                new CompositePartitionQuerySet(10, 10),
                                                                                new CompositePartitionQuerySet(10, Integer.MAX_VALUE),
                                                                                new CompositePartitionQuerySet(24, 10),
                                                                                new CompositePartitionQuerySet(24, 100),
                                                                                new CompositePartitionQuerySet(24, Integer.MAX_VALUE));

    static List<BaseQuerySet> STATIC_QUERY_SETS = ImmutableList.of(new StaticColumnQuerySet(10, 5),
                                                                   new StaticColumnQuerySet(10, 10),
                                                                   new StaticColumnQuerySet(10, Integer.MAX_VALUE),
                                                                   new StaticColumnQuerySet(24, 10),
                                                                   new StaticColumnQuerySet(24, 100),
                                                                   new StaticColumnQuerySet(24, Integer.MAX_VALUE));

    static final Injections.Counter INDEX_QUERY_COUNTER = Injections.newCounter("IndexQueryCounter")
                                                                    .add(newInvokePoint().onClass(StorageAttachedIndexSearcher.class).onMethod("search"))
                                                                    .build();

    @Parameterized.Parameter(0)
    public DataModel dataModel;
    @Parameterized.Parameter(1)
    public List<BaseQuerySet> sets;

    @Before
    public void setup() throws Throwable
    {
        requireNetwork();

        Injections.inject(INDEX_QUERY_COUNTER);
    }

    protected void writeLifecycle() throws Throwable
    {
        dataModel.createTables(this);

        dataModel.disableCompaction(this);

        dataModel.createIndexes(this);

        // queries against Memtable adjacent in-memory indexes
        dataModel.insertRows(this);
        executeQueries(dataModel, sets);

        // queries with Memtable flushed to SSTable on disk
        dataModel.flush(this);
        executeQueries(dataModel, sets);

        // queries across memory and disk indexes
        dataModel.insertRows(this);
        executeQueries(dataModel, sets);

        // queries w/ multiple SSTable indexes
        dataModel.flush(this);
        executeQueries(dataModel, sets);

        // queries after compacting to a single SSTable index
        dataModel.compact(this);
        executeQueries(dataModel, sets);

        // queries against Memtable updates and the existing SSTable index
        dataModel.updateCells(this);
        executeQueries(dataModel, sets);

        // queries against the newly flushed SSTable index and the existing SSTable index
        dataModel.flush(this);
        executeQueries(dataModel, sets);

        // queries after compacting updates into to a single SSTable index
        dataModel.compact(this);
        executeQueries(dataModel, sets);
    }

    public void rowDeletions() throws Throwable
    {
        dataModel.createTables(this);

        dataModel.disableCompaction(this);

        dataModel.createIndexes(this);
        dataModel.insertRows(this);
        dataModel.flush(this);
        dataModel.compact(this);

        // baseline queries
        executeQueries(dataModel, sets);

        // queries against Memtable deletes and the existing SSTable index
        dataModel.deleteRows(this);
        executeQueries(dataModel, sets);

        // queries against the newly flushed SSTable index and the existing SSTable index
        dataModel.flush(this);
        executeQueries(dataModel, sets);

        // queries after compacting deletes into to a single SSTable index
        dataModel.compact(this);
        executeQueries(dataModel, sets);

        // truncate, reload, and verify that the load is clean
        dataModel.truncateTables(this);
        dataModel.insertRows(this);
        executeQueries(dataModel, sets);
    }

    public void cellDeletions() throws Throwable
    {
        dataModel.createTables(this);

        dataModel.disableCompaction(this);

        dataModel.createIndexes(this);
        dataModel.insertRows(this);
        dataModel.flush(this);
        dataModel.compact(this);

        // baseline queries
        executeQueries(dataModel, sets);

        // queries against Memtable deletes and the existing SSTable index
        dataModel.deleteCells(this);
        executeQueries(dataModel, sets);

        // queries against the newly flushed SSTable index and the existing SSTable index
        dataModel.flush(this);
        executeQueries(dataModel, sets);

        // queries after compacting deletes into to a single SSTable index
        dataModel.compact(this);
        executeQueries(dataModel, sets);
    }

    public void timeToLive() throws Throwable
    {
        dataModel.createTables(this);

        dataModel.disableCompaction(this);

        dataModel.createIndexes(this);
        dataModel.insertRowsWithTTL(this);

        // Wait for the TTL to become effective:
        TimeUnit.SECONDS.sleep(DataModel.DEFAULT_TTL_SECONDS);

        // Make sure TTLs are reflected in our query results from the Memtable:
        executeQueries(dataModel, sets);

        // Make sure TTLs are reflected in our query results from SSTables:
        dataModel.flush(this);
        executeQueries(dataModel, sets);

        // Make sure fresh overwrites invalidate TTLs:
        dataModel.insertRows(this);
        executeQueries(dataModel, sets);
    }

    @SuppressWarnings("unused")
    @Parameterized.Parameters(name = "{0}")
    public static List<Object[]> params() throws Throwable
    {
        List<Object[]> scenarios = new LinkedList<>();

        scenarios.add(new Object[]{ new DataModel.BaseDataModel(DataModel.NORMAL_COLUMNS, DataModel.NORMAL_COLUMN_DATA), BASE_QUERY_SETS });

        scenarios.add(new Object[]{ new DataModel.CompoundKeyDataModel(DataModel.NORMAL_COLUMNS, DataModel.NORMAL_COLUMN_DATA), BASE_QUERY_SETS });

        scenarios.add(new Object[]{ new DataModel.CompoundKeyWithStaticsDataModel(DataModel.STATIC_COLUMNS, DataModel.STATIC_COLUMN_DATA), STATIC_QUERY_SETS });

        scenarios.add(new Object[]{ new DataModel.CompositePartitionKeyDataModel(DataModel.NORMAL_COLUMNS, DataModel.NORMAL_COLUMN_DATA),
                                    ImmutableList.builder().addAll(BASE_QUERY_SETS).addAll(COMPOSITE_PARTITION_QUERY_SETS).build()});

        return scenarios;
    }

    static String randomPostfix()
    {
        return UUID.randomUUID().toString().replace("-", "_");
    }

    private void executeQueries(DataModel dataModel, List<BaseQuerySet> sets) throws Throwable
    {
        for (BaseQuerySet set : sets)
        {
            set.execute(this, dataModel);
        }
    }

    static class StaticColumnQuerySet extends BaseQuerySet
    {
        StaticColumnQuerySet(int limit, int fetchSize)
        {
            super(limit, fetchSize);
        }

        public void execute(SAITester tester, DataModel model) throws Throwable
        {
            super.execute(tester, model);

            query(tester, model, DataModel.STATIC_INT_COLUMN, Operator.EQ, 1845);
            query(tester, model, DataModel.STATIC_INT_COLUMN, Operator.LT, 1845);
            query(tester, model, DataModel.STATIC_INT_COLUMN, Operator.LTE, 1845);
            query(tester, model, DataModel.STATIC_INT_COLUMN, Operator.GT, 1845);
            query(tester, model, DataModel.STATIC_INT_COLUMN, Operator.GTE, 1845);
            query(tester, model, DataModel.STATIC_INT_COLUMN, Operator.EQ, 1909);
            query(tester, model, DataModel.STATIC_INT_COLUMN, Operator.LT, 1787);
            query(tester, model, DataModel.STATIC_INT_COLUMN, Operator.GT, 1910);

            rangeQuery(tester, model, DataModel.STATIC_INT_COLUMN, 1845, 1909);
        }
    }

    static class CompositePartitionQuerySet extends BaseQuerySet
    {
        CompositePartitionQuerySet(int limit, int fetchSize)
        {
            super(limit, fetchSize);
        }

        public void execute(SAITester tester, DataModel model) throws Throwable
        {
            super.execute(tester, model);

            DataModel.BaseDataModel baseDataModel = (DataModel.BaseDataModel) model;
            for(Pair<String, String> partitionKeyComponent: baseDataModel.keyColumns)
            {
                String partitionKeyComponentName = partitionKeyComponent.left;
                query(tester, model, partitionKeyComponentName, Operator.EQ, 0);
                query(tester, model, partitionKeyComponentName, Operator.GT, 0);
                query(tester, model, partitionKeyComponentName, Operator.LTE, 2);
                query(tester, model, partitionKeyComponentName, Operator.GTE, -1);
                query(tester, model, partitionKeyComponentName, Operator.LT, 50);
                query(tester, model, partitionKeyComponentName, Operator.GT, 0);
            }

            String firstPartitionKey = baseDataModel.keyColumns.get(0).left;
            String secondPartitionKey = baseDataModel.keyColumns.get(1).left;
            List<Operator> numericOperators = Arrays.asList(Operator.EQ, Operator.GT, Operator.LT, Operator.GTE, Operator.LTE);
            List<List<Operator>> combinations = Lists.cartesianProduct(numericOperators, numericOperators).stream()
                                                     .filter(p-> p.get(0) != Operator.EQ || p.get(1) != Operator.EQ) //If both are EQ the entire partition is specified
                                                     .collect(Collectors.toList());
            for(List<Operator> operators : combinations)
            {
                andQuery(tester,
                         model,
                         firstPartitionKey, operators.get(0), 2,
                         secondPartitionKey, operators.get(1), 2,
                         false);
            }
        }
    }

    private static class BaseQuerySet
    {
        final int limit;
        final int fetchSize;

        BaseQuerySet(int limit, int fetchSize)
        {
            this.limit = limit;
            this.fetchSize = fetchSize;
        }

        void execute(SAITester tester, DataModel model) throws Throwable
        {
            query(tester, model, DataModel.ASCII_COLUMN, Operator.EQ, "MA");
            query(tester, model, DataModel.ASCII_COLUMN, Operator.EQ, "LA");
            query(tester, model, DataModel.ASCII_COLUMN, Operator.EQ, "XX");

            query(tester, model, DataModel.BIGINT_COLUMN, Operator.EQ, 4800000000L);
            query(tester, model, DataModel.BIGINT_COLUMN, Operator.EQ, 5000000000L);
            query(tester, model, DataModel.BIGINT_COLUMN, Operator.LT, 5000000000L);
            query(tester, model, DataModel.BIGINT_COLUMN, Operator.LTE, 5000000000L);
            query(tester, model, DataModel.BIGINT_COLUMN, Operator.GT, 5000000000L);
            query(tester, model, DataModel.BIGINT_COLUMN, Operator.GTE, 5000000000L);
            query(tester, model, DataModel.BIGINT_COLUMN, Operator.EQ, 22L);
            query(tester, model, DataModel.BIGINT_COLUMN, Operator.LT, 400000000L);
            query(tester, model, DataModel.BIGINT_COLUMN, Operator.GT, 10000000000L);

            rangeQuery(tester, model, DataModel.BIGINT_COLUMN, 3000000000L, 7000000000L);

            query(tester, model, DataModel.DATE_COLUMN, Operator.EQ, SimpleDateType.instance.fromString("2013-06-10"));
            query(tester, model, DataModel.DATE_COLUMN, Operator.EQ, SimpleDateType.instance.fromString("2013-06-17"));
            query(tester, model, DataModel.DATE_COLUMN, Operator.LT, SimpleDateType.instance.fromString("2013-06-17"));
            query(tester, model, DataModel.DATE_COLUMN, Operator.LTE, SimpleDateType.instance.fromString("2013-06-17"));
            query(tester, model, DataModel.DATE_COLUMN, Operator.GT, SimpleDateType.instance.fromString("2013-06-17"));
            query(tester, model, DataModel.DATE_COLUMN, Operator.GTE, SimpleDateType.instance.fromString("2013-06-17"));
            query(tester, model, DataModel.DATE_COLUMN, Operator.EQ, SimpleDateType.instance.fromString("2017-01-01"));
            query(tester, model, DataModel.DATE_COLUMN, Operator.LT, SimpleDateType.instance.fromString("2000-01-01"));
            query(tester, model, DataModel.DATE_COLUMN, Operator.GT, SimpleDateType.instance.fromString("2020-01-01"));

            rangeQuery(tester, model, DataModel.DATE_COLUMN, SimpleDateType.instance.fromString("2013-06-17"), SimpleDateType.instance.fromString("2018-06-19"));

            query(tester, model, DataModel.DOUBLE_COLUMN, Operator.EQ, 43203.90);
            query(tester, model, DataModel.DOUBLE_COLUMN, Operator.EQ, 7800.06);
            query(tester, model, DataModel.DOUBLE_COLUMN, Operator.LT, 82169.62);
            query(tester, model, DataModel.DOUBLE_COLUMN, Operator.LTE, 82169.62);
            query(tester, model, DataModel.DOUBLE_COLUMN, Operator.GT, 82169.62);
            query(tester, model, DataModel.DOUBLE_COLUMN, Operator.GTE, 82169.62);
            query(tester, model, DataModel.DOUBLE_COLUMN, Operator.EQ, 82169.60);
            query(tester, model, DataModel.DOUBLE_COLUMN, Operator.LT, 1948.54);
            query(tester, model, DataModel.DOUBLE_COLUMN, Operator.GT, 570640.95);

            rangeQuery(tester, model, DataModel.DOUBLE_COLUMN, 56538.90, 113594.08);

            query(tester, model, DataModel.FLOAT_COLUMN, Operator.EQ, 10.2f);
            query(tester, model, DataModel.FLOAT_COLUMN, Operator.EQ, 1.9f);
            query(tester, model, DataModel.FLOAT_COLUMN, Operator.LT, 5.3f);
            query(tester, model, DataModel.FLOAT_COLUMN, Operator.LTE, 5.3f);
            query(tester, model, DataModel.FLOAT_COLUMN, Operator.GT, 5.3f);
            query(tester, model, DataModel.FLOAT_COLUMN, Operator.GTE, 5.3f);
            query(tester, model, DataModel.FLOAT_COLUMN, Operator.EQ, 5.9f);
            query(tester, model, DataModel.FLOAT_COLUMN, Operator.LT, 1.8f);
            query(tester, model, DataModel.FLOAT_COLUMN, Operator.GT, 10.2f);

            rangeQuery(tester, model, DataModel.FLOAT_COLUMN, 4.6f, 6.7f);

            query(tester, model, INET_COLUMN, Operator.EQ, InetAddressType.instance.fromString("170.63.206.57"));
            query(tester, model, INET_COLUMN, Operator.EQ, InetAddressType.instance.fromString("170.63.206.56"));
            query(tester, model, INET_COLUMN, Operator.EQ, InetAddressType.instance.fromString("205.204.196.65"));
            query(tester, model, INET_COLUMN, Operator.EQ, InetAddressType.instance.fromString("164.165.67.10"));
            query(tester, model, INET_COLUMN, Operator.EQ, InetAddressType.instance.fromString("204.196.242.71"));

            rangeQuery(tester, model, DataModel.INT_COLUMN, 2977853, 6784240);

            query(tester, model, DataModel.SMALLINT_COLUMN, Operator.EQ, (short) 164);
            query(tester, model, DataModel.SMALLINT_COLUMN, Operator.LT, (short) 164);
            query(tester, model, DataModel.SMALLINT_COLUMN, Operator.LTE, (short) 164);
            query(tester, model, DataModel.SMALLINT_COLUMN, Operator.GT, (short) 164);
            query(tester, model, DataModel.SMALLINT_COLUMN, Operator.GTE, (short) 164);
            query(tester, model, DataModel.SMALLINT_COLUMN, Operator.EQ, (short) 2);
            query(tester, model, DataModel.SMALLINT_COLUMN, Operator.LT, (short) 30);
            query(tester, model, DataModel.SMALLINT_COLUMN, Operator.GT, (short) 1861);

            rangeQuery(tester, model, DataModel.SMALLINT_COLUMN, (short) 126, (short) 383);

            query(tester, model, DataModel.TINYINT_COLUMN, Operator.EQ, (byte) 16);
            query(tester, model, DataModel.TINYINT_COLUMN, Operator.LT, (byte) 16);
            query(tester, model, DataModel.TINYINT_COLUMN, Operator.LTE, (byte) 16);
            query(tester, model, DataModel.TINYINT_COLUMN, Operator.GT, (byte) 16);
            query(tester, model, DataModel.TINYINT_COLUMN, Operator.GTE, (byte) 16);
            query(tester, model, DataModel.TINYINT_COLUMN, Operator.EQ, (byte) 1);
            query(tester, model, DataModel.TINYINT_COLUMN, Operator.LT, (byte) 2);
            query(tester, model, DataModel.TINYINT_COLUMN, Operator.GT, (byte) 117);

            rangeQuery(tester, model, DataModel.TINYINT_COLUMN, (byte) 12, (byte) 47);

            query(tester, model, DataModel.TEXT_COLUMN, Operator.EQ, "Alaska");
            query(tester, model, DataModel.TEXT_COLUMN, Operator.EQ, "Wyoming");
            query(tester, model, DataModel.TEXT_COLUMN, Operator.EQ, "Franklin");
            query(tester, model, DataModel.TEXT_COLUMN, Operator.EQ, "State of Michigan");
            query(tester, model, DataModel.TEXT_COLUMN, Operator.EQ, "Michigan");
            query(tester, model, DataModel.TEXT_COLUMN, Operator.EQ, "Louisiana");
            query(tester, model, DataModel.TEXT_COLUMN, Operator.EQ, "Massachusetts");

            query(tester, model, DataModel.TIME_COLUMN, Operator.EQ, TimeType.instance.fromString("00:43:07"));
            query(tester, model, DataModel.TIME_COLUMN, Operator.LT, TimeType.instance.fromString("00:43:07"));
            query(tester, model, DataModel.TIME_COLUMN, Operator.LTE, TimeType.instance.fromString("00:43:07"));
            query(tester, model, DataModel.TIME_COLUMN, Operator.GT, TimeType.instance.fromString("00:43:07"));
            query(tester, model, DataModel.TIME_COLUMN, Operator.GTE, TimeType.instance.fromString("00:43:07"));
            query(tester, model, DataModel.TIME_COLUMN, Operator.EQ, TimeType.instance.fromString("00:15:57"));
            query(tester, model, DataModel.TIME_COLUMN, Operator.LT, TimeType.instance.fromString("00:15:50"));
            query(tester, model, DataModel.TIME_COLUMN, Operator.GT, TimeType.instance.fromString("01:30:45"));

            rangeQuery(tester, model, DataModel.TIME_COLUMN, TimeType.instance.fromString("00:38:13"), TimeType.instance.fromString("00:56:07"));

            query(tester, model, DataModel.TIMESTAMP_COLUMN, Operator.EQ, TimestampType.instance.fromString("2013-06-17T00:00:00"));
            query(tester, model, DataModel.TIMESTAMP_COLUMN, Operator.LT, TimestampType.instance.fromString("2013-06-17T00:00:00"));
            query(tester, model, DataModel.TIMESTAMP_COLUMN, Operator.LTE, TimestampType.instance.fromString("2013-06-17T00:00:00"));
            query(tester, model, DataModel.TIMESTAMP_COLUMN, Operator.GT, TimestampType.instance.fromString("2013-06-17T00:00:00"));
            query(tester, model, DataModel.TIMESTAMP_COLUMN, Operator.GTE, TimestampType.instance.fromString("2013-06-17T00:00:00"));
            query(tester, model, DataModel.TIMESTAMP_COLUMN, Operator.EQ, TimestampType.instance.fromString("2017-01-01T00:00:00"));
            query(tester, model, DataModel.TIMESTAMP_COLUMN, Operator.LT, TimestampType.instance.fromString("2000-01-01T00:00:00"));
            query(tester, model, DataModel.TIMESTAMP_COLUMN, Operator.GT, TimestampType.instance.fromString("2020-01-01T00:00:00"));

            rangeQuery(tester, model, DataModel.TIMESTAMP_COLUMN,
                       TimestampType.instance.fromString("2013-6-17T00:00:00"),
                       TimestampType.instance.fromString("2018-6-19T00:00:00"));

            query(tester, model, DataModel.UUID_COLUMN, Operator.EQ, UUIDType.instance.fromString("e37394dc-d17b-11e8-a8d5-f2801f1b9fd1"));
            query(tester, model, DataModel.UUID_COLUMN, Operator.EQ, UUIDType.instance.fromString("752355f8-405b-4d94-88f3-9992cda30f1e"));
            query(tester, model, DataModel.UUID_COLUMN, Operator.EQ, UUIDType.instance.fromString("ac0aa734-d17f-11e8-a8d5-f2801f1b9fd1"));
            query(tester, model, DataModel.UUID_COLUMN, Operator.EQ, UUIDType.instance.fromString("c6eec0b0-0eef-40e8-ac38-3a82110443e4"));
            query(tester, model, DataModel.UUID_COLUMN, Operator.EQ, UUIDType.instance.fromString("e37394dc-d17b-11e8-a8d5-f2801f1b9fd1"));

            query(tester, model, DataModel.TIMEUUID_COLUMN, Operator.EQ, UUIDType.instance.fromString("ee6136d2-d17c-11e8-a8d5-f2801f1b9fd1"));
            query(tester, model, DataModel.TIMEUUID_COLUMN, Operator.LT, UUIDType.instance.fromString("ee6136d2-d17c-11e8-a8d5-f2801f1b9fd1"));
            query(tester, model, DataModel.TIMEUUID_COLUMN, Operator.LTE, UUIDType.instance.fromString("ee6136d2-d17c-11e8-a8d5-f2801f1b9fd1"));
            query(tester, model, DataModel.TIMEUUID_COLUMN, Operator.GT, UUIDType.instance.fromString("ee6136d2-d17c-11e8-a8d5-f2801f1b9fd1"));
            query(tester, model, DataModel.TIMEUUID_COLUMN, Operator.GTE, UUIDType.instance.fromString("ee6136d2-d17c-11e8-a8d5-f2801f1b9fd1"));
            query(tester, model, DataModel.TIMEUUID_COLUMN, Operator.EQ, UUIDType.instance.fromString("2a421a68-d182-11e8-a8d5-f2801f1b9fd1"));

            andQuery(tester, model,
                     DataModel.TIMESTAMP_COLUMN, Operator.GTE, TimestampType.instance.fromString("2013-06-20T00:00:00"),
                     DataModel.UUID_COLUMN, Operator.EQ, UUIDType.instance.fromString("752355f8-405b-4d94-88f3-9992cda30f1e"),
                     false);

            andQuery(tester, model,
                     DataModel.TIMESTAMP_COLUMN, Operator.GTE, TimestampType.instance.fromString("2018-06-20T00:00:00"),
                     DataModel.TEXT_COLUMN, Operator.EQ, "Texas",
                     false);

            andQuery(tester, model,
                     DataModel.SMALLINT_COLUMN, Operator.LTE, (short) 126,
                     DataModel.TINYINT_COLUMN, Operator.LTE, (byte) 9,
                     false);

            andQuery(tester, model,
                     DataModel.SMALLINT_COLUMN, Operator.LTE, (short) 126,
                     DataModel.NON_INDEXED_COLUMN, Operator.GT, 0,
                     true);

            andQuery(tester, model,
                     DataModel.TEXT_COLUMN, Operator.EQ, "Alaska",
                     DataModel.NON_INDEXED_COLUMN, Operator.EQ, 2,
                     true);


            andQuery(tester, model,
                     DataModel.UUID_COLUMN, Operator.EQ, UUIDType.instance.fromString("e37394dc-d17b-11e8-a8d5-f2801f1b9fd1"),
                     DataModel.NON_INDEXED_COLUMN, Operator.LT, 3,
                     true);

            // with partition column filtering
            String firstPartitionKey = model.keyColumns().get(0).left;

            andQuery(tester, model,
                     DataModel.TEXT_COLUMN, Operator.EQ, "Alaska",
                     firstPartitionKey, Operator.EQ, 0,
                     true);

            andQuery(tester, model,
                     DataModel.TEXT_COLUMN, Operator.EQ, "Kentucky",
                     firstPartitionKey, Operator.GT, 4,
                     true);

            andQuery(tester, model,
                     DataModel.TEXT_COLUMN, Operator.EQ, "Wyoming",
                     firstPartitionKey, Operator.LT, 200,
                     true);

            if (model.keyColumns().size() > 1)
            {
                String secondPrimaryKey = model.keyColumns().get(1).left;

                andQuery(tester, model,
                         DataModel.BIGINT_COLUMN, Operator.EQ, 4800000000L,
                         secondPrimaryKey, Operator.EQ, 0,
                         true);

                andQuery(tester, model,
                         DataModel.DOUBLE_COLUMN, Operator.EQ, 82169.60,
                         secondPrimaryKey, Operator.GT, 0,
                         true);

                andQuery(tester, model,
                         DataModel.DOUBLE_COLUMN, Operator.LT, 1948.54,
                         secondPrimaryKey, Operator.LTE, 2,
                         true);

                andQuery(tester, model,
                         DataModel.TEXT_COLUMN, Operator.EQ, "Alaska",
                         firstPartitionKey, Operator.EQ, 0,
                         secondPrimaryKey, Operator.GTE, -1);

                andQuery(tester, model,
                         DataModel.TEXT_COLUMN, Operator.EQ, "Kentucky",
                         firstPartitionKey, Operator.GT, 4,
                         secondPrimaryKey, Operator.LT, 50);

                andQuery(tester, model,
                         DataModel.TEXT_COLUMN, Operator.EQ, "Wyoming",
                         firstPartitionKey, Operator.LT, 200,
                         secondPrimaryKey, Operator.GT, 0);
            }
        }

        void query(SAITester tester, DataModel model, String column, Operator operator, Object value) throws Throwable
        {
            String query = String.format(DataModel.SIMPLE_SELECT_TEMPLATE, DataModel.ASCII_COLUMN, column, operator);
            String queryValidator = String.format(DataModel.SIMPLE_SELECT_WITH_FILTERING_TEMPLATE, DataModel.ASCII_COLUMN, column, operator);
            validate(tester, model, query, queryValidator, value, limit);
        }

        void andQuery(SAITester tester, DataModel model,
                      String column1, Operator operator1, Object value1,
                      String column2, Operator operator2, Object value2,
                      boolean filtering) throws Throwable
        {
            String query = String.format(filtering ? DataModel.TWO_CLAUSE_AND_QUERY_FILTERING_TEMPLATE : DataModel.TWO_CLAUSE_AND_QUERY_TEMPLATE,
                                         DataModel.ASCII_COLUMN, column1, operator1, column2, operator2);

            String queryValidator = String.format(DataModel.TWO_CLAUSE_AND_QUERY_FILTERING_TEMPLATE,
                                                  DataModel.ASCII_COLUMN, column1, operator1, column2, operator2);

            validate(tester, model,query, queryValidator, value1, value2, limit);
        }

        void andQuery(SAITester tester, DataModel model,
                      String column1, Operator operator1, Object value1,
                      String column2, Operator operator2, Object value2,
                      String column3, Operator operator3, Object value3) throws Throwable
        {
            // TODO: If we support indexes in all columns, ALLOW FILTERING might go away here...
            String query = String.format(DataModel.THREE_CLAUSE_AND_QUERY_FILTERING_TEMPLATE,
                                         DataModel.ASCII_COLUMN, column1, operator1, column2, operator2, column3, operator3);

            String queryValidator = String.format(DataModel.THREE_CLAUSE_AND_QUERY_FILTERING_TEMPLATE,
                                                  DataModel.ASCII_COLUMN, column1, operator1, column2, operator2, column3, operator3);

            validate(tester, model, query, queryValidator, value1, value2, value3, limit);
        }

        void rangeQuery(SAITester tester, DataModel model, String column, Object value1, Object value2) throws Throwable
        {
            String template = "SELECT %s FROM %%s WHERE %s > ? AND %s < ? LIMIT ?";
            String templateWithFiltering = "SELECT %s FROM %%s WHERE %s > ? AND %s < ? LIMIT ? ALLOW FILTERING";

            String query = String.format(template, DataModel.ASCII_COLUMN, column, column);
            String queryValidator = String.format(templateWithFiltering, DataModel.ASCII_COLUMN, column, column);
            validate(tester, model, query, queryValidator, value1, value2, limit);
        }

        private List<Object> validate(SAITester tester, DataModel model, String query, String validator, Object... values) throws Throwable
        {
            try
            {
                INDEX_QUERY_COUNTER.reset();

                List<Object> actual = model.executeIndexed(tester, query, fetchSize, values).all().stream().map(r -> r.getObject(0)).collect(Collectors.toList());

                // This could be more strict, but it serves as a reasonable paging-aware lower bound:
                int pageCount = (int) Math.ceil(actual.size() / (double) Math.min(actual.size(), fetchSize));
                assertThat("Expected more calls to " + StorageAttachedIndexSearcher.class, INDEX_QUERY_COUNTER.get(), Matchers.greaterThanOrEqualTo((long) Math.max(1, pageCount)));

                List<Object> expected = model.executeNonIndexed(tester, validator, fetchSize, values).all().stream().map(r -> r.getObject(0)).collect(Collectors.toList());

                assertEquals(expected, actual);

                return expected;
            }
            catch (Throwable ex)
            {
                // When thrown here, AssertionError does not seem to produce a stack trace, so it's logged explicitly:
                logger.error("Validation failed while executing query: " + query + ", exception message: " + ex.getMessage(), ex);
                throw ex;
            }
        }

        @Override
        public String toString()
        {
            return MoreObjects.toStringHelper(this).add("limit", limit).add("fetchSize", fetchSize).toString();
        }
    }
}
