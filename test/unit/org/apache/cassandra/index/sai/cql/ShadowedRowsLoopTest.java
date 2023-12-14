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


import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import javax.management.JMX;
import javax.management.ObjectName;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.index.sai.plan.QueryController;
import org.apache.cassandra.index.sai.plan.VectorTopKProcessor;
import org.apache.cassandra.metrics.CassandraMetricsRegistry;

import static org.assertj.core.api.Assertions.assertThat;


@RunWith(Parameterized.class)
public class ShadowedRowsLoopTest extends VectorTester
{
    private static final String PER_QUERY_METRIC_TYPE = "PerQuery";

    final static int vectorCount = 50;
    final static int dimension = 10;
    final int N;
    final int isOnDisk;

    final static int MAX_LIMIT = 200;
    final int liveVectorsNum;
    private Vector<Float> queryVector;

    @BeforeClass
    public static void beforeClass() throws Exception
    {
        startJMXServer();
        createMBeanServerConnection();
    }

    @Parameterized.Parameters
    public static Object[][] data()
    {
        List<Object[]> result = new ArrayList<>();
        for (int N: new int[]{ 1, 5, 13 } )
        {
            for (int isOnDisk: new int[]{ 0, 1, 2, 3 })
            {
                result.add(new Object[]{ N, isOnDisk });
            }
        }

        return result.toArray(Object[][]::new);
    }

    public ShadowedRowsLoopTest(int N, int isOnDisk)
    {
        this.N = N;
        this.isOnDisk = isOnDisk;
        this.liveVectorsNum = vectorCount + MAX_LIMIT;
    }

    @Before
    public void beforeTest() throws Throwable
    {
        super.beforeTest();

        createTable(String.format("CREATE TABLE %%s (pk int, str_val text, val vector<float, %d>, PRIMARY KEY(pk))", dimension));
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");
        waitForIndexQueryable();
        disableCompaction();

        switch (isOnDisk)
        {
            case 0:
                prepareDataOnDisk();
                break;
            case 1:
                prepareDataInMemtable();
                break;
            case 2:
                prepareDataMixedDelsInMemory();
                break;
            case 3:
                prepareDataMixedDelsInMemoryAndDisk();
                break;
            default:
                throw new IllegalArgumentException("Unknown isOnDisk value: " + isOnDisk);
        }

        this.queryVector = randomVector();
    }

    private void prepareDataMixedDelsInMemoryAndDisk()
    {
        prepareDataOnDisk();

        // delete some, don't flush
        for (int i = liveVectorsNum; i < liveVectorsNum + vectorCount / 2; i += 2)
        {
            execute("DELETE FROM %s WHERE pk = ?", i);
        }
    }

    private void prepareDataMixedDelsInMemory()
    {
        // insert some, flush
        for (int i = liveVectorsNum; i < 2 * liveVectorsNum; i++)
        {
            execute("INSERT INTO %s (pk, str_val, val) VALUES (?, ?, ?)",
                    i, Integer.toString(i), randomVector());

        }
        flush();

        // delete some, don't flush
        for (int i = liveVectorsNum; i < liveVectorsNum + vectorCount / 2; i += 2)
        {
            execute("DELETE FROM %s WHERE pk = ?", i);
        }
    }

    private void prepareDataInMemtable()
    {
        // insert some, delete some, don't flush
        for (int i = liveVectorsNum; i < 2 * liveVectorsNum; i++)
        {
            execute("INSERT INTO %s (pk, str_val, val) VALUES (?, ?, ?)",
                    i, Integer.toString(i), randomVector());

            if (i % 2 == 0)
                execute("DELETE FROM %s WHERE pk = ?", i);
        }
    }

    private void prepareDataOnDisk()
    {
        // insert records with pk starting at vectorCount, flush
        // these will be returned by search
        for (int i = liveVectorsNum; i < 2 * liveVectorsNum; i++)
        {
            execute("INSERT INTO %s (pk, str_val, val) VALUES (?, ?, ?)",
                    i, Integer.toString(i), randomVector());
        }
        flush();

        //insert/delete records to force shadowed rows loop
        for (int loop = 0; loop < N; loop++)
        {
            // insert records with pk starting at 0 and < vectorCount, flush
            for (int i = 0; i < vectorCount; i++)
            {
                execute("INSERT INTO %s (pk, str_val, val) VALUES (?, ?, ?)",
                        i, Integer.toString(i), randomVector());
            }
            flush();

            // delete records with pk starting at 0 and < vectorCount, flush
            // now the records are shadowed
            for (int i = 0; i < vectorCount; i++)
            {
                execute("DELETE FROM %s WHERE pk = ?", i);
            }
            flush();
        }
    }

    @Test
    public void shadowedLoopTest() throws Throwable
    {
        // to speed up test/reduce time on data preparation
        for (int limit: new int[]{ 1, 2, 5, 20, 50, MAX_LIMIT })
        {
            shadowedLoopTest(limit);
        }
    }

    public void shadowedLoopTest(int limit) throws Throwable
    {
        resetMetrics();

        QueryController.allowSpeculativeLimits = false;
        search(queryVector, limit);
        Metrics resultNoSp = getMetrics();
        assertThat(resultNoSp.loops).isGreaterThan(0);

        resetMetrics();

        QueryController.allowSpeculativeLimits = true;
        search(queryVector, limit);
        Metrics result = getMetrics();
        assertThat(result.loops).isGreaterThan(0);

        logger.info("OnDisk: {} N: {}, limit: {}; Got loops {} -> {}",
                    toPrintable(isOnDisk), N, limit, resultNoSp.loops, result.loops);

        // vector search has some randomness which can result in hitting more shadowed rows
        // than expected. Allow for some extra loops.
        int allowExtraLoop = result.keys > resultNoSp.keys ? 1 : 0;;
        if (resultNoSp.loops > 3)
            assertThat(result.loops).isLessThan(resultNoSp.loops + allowExtraLoop);
        else
            assertThat(result.loops).isLessThanOrEqualTo(resultNoSp.loops + allowExtraLoop);
    }

    private String toPrintable(int isOnDisk)
    {
        switch (isOnDisk)
        {
            case 0:
                return "OnDisk    ";
            case 1:
                return "InMemtable";
            case 2:
                return "DelsInMem ";
            case 3:
                return "DelsInAny ";
            default:
                return "UNKNOWN";
        }
    }

    private Metrics getMetrics() throws InterruptedException
    {
        long prev = -1;
        long loops = -1;
        long keys = -1;

        // poll for metric to be updated
        for (int i = 0; i < 100; i++)
        {
            var loopsMetric = getQueryHistogram("ShadowedKeysLoopsHistogram");
            var keysMetric = getQueryHistogram("ShadowedKeysScannedHistogram");
            loops = loopsMetric.getMax();
            keys = keysMetric.getMax();

            if (loops > 0 && loops == prev)
                break;
            prev = loops;
            loops = -1;
            keys = -1;
            Thread.sleep(100);
        }
        return new Metrics(loops, keys);
    }

    private void resetMetrics() throws InterruptedException
    {
        long loops;
        getQueryHistogram("ShadowedKeysLoopsHistogram").clear();
        getQueryHistogram("ShadowedKeysScannedHistogram").clear();

        // poll for metric to be reset
        for (int i = 0; i < 100; i++)
        {
            var loopsNoSpMetric = getQueryHistogram("ShadowedKeysLoopsHistogram");
            loops = loopsNoSpMetric.getMax();

            if (loops == 0)
                break;
            Thread.sleep(100);
        }
        assertThat(getQueryHistogram("ShadowedKeysLoopsHistogram").getMax()).isEqualTo(0);
    }

    private static class Metrics
    {
        public final long loops;
        public final long keys;

        public Metrics(long loops, long keys)
        {
            this.loops = loops;
            this.keys = keys;
        }
    }

    private UntypedResultSet search(Vector<Float> queryVector, int limit) throws Throwable
    {
        return execute("SELECT * FROM %s ORDER BY val ann of ? LIMIT " + limit, queryVector);
    }

    private CassandraMetricsRegistry.JmxHistogramMBean getQueryHistogram(String metricName)
    {
        ObjectName oName = objectNameNoIndex(metricName, keyspace(), currentTable(), PER_QUERY_METRIC_TYPE);
        return JMX.newMBeanProxy(jmxConnection, oName, CassandraMetricsRegistry.JmxHistogramMBean.class);
    }

    private Vector<Float> randomVector()
    {
        Float[] rawVector = new Float[dimension];
        for (int i = 0; i < dimension; i++)
        {
            rawVector[i] = getRandom().nextFloat();
        }
        return new Vector<>(rawVector);
    }
}
