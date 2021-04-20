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
package org.apache.cassandra.index.sai.cql.types;

import java.util.Arrays;
import java.util.Collection;

import org.junit.Before;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.index.sai.SAITester;

public abstract class IndexingTypeSupport extends SAITester
{
    public static final int NUMBER_OF_VALUES = 64;

    protected final DataSet<?> dataset;

    private final boolean widePartitions;
    private final Scenario scenario;
    private Object[][] allRows;

    public enum Scenario
    {
        MEMTABLE_QUERY,
        SSTABLE_QUERY,
        MIXED_QUERY,
        COMPACTED_QUERY,
        POST_BUILD_QUERY
    }

    protected static Collection<Object[]> generateParameters(DataSet<?> dataset)
    {
        return Arrays.asList(new Object[][]
        {
            { dataset, true, Scenario.MEMTABLE_QUERY },
            { dataset, true, Scenario.SSTABLE_QUERY},
            { dataset, true, Scenario.COMPACTED_QUERY},
            { dataset, true, Scenario.MIXED_QUERY},
            { dataset, true, Scenario.POST_BUILD_QUERY},
            { dataset, false, Scenario.MEMTABLE_QUERY },
            { dataset, false, Scenario.SSTABLE_QUERY},
            { dataset, false, Scenario.COMPACTED_QUERY},
            { dataset, false, Scenario.MIXED_QUERY},
            { dataset, false, Scenario.POST_BUILD_QUERY}
        });
    }

    public IndexingTypeSupport(DataSet<?> dataset, boolean widePartitions, Scenario scenario)
    {
        this.dataset = dataset;
        this.widePartitions = widePartitions;
        this.scenario = scenario;
    }

    @Before
    public void createTable()
    {
        dataset.init();

        createTable(String.format("CREATE TABLE %%s (pk int, ck int, value %s, PRIMARY KEY(pk, ck))", dataset));

        disableCompaction();

        allRows = generateRows(dataset, widePartitions);
    }

    protected void runIndexQueryScenarios() throws Throwable
    {
        if (scenario != Scenario.POST_BUILD_QUERY)
        {
            for (String index : dataset.decorateIndexColumn("value"))
                createIndex(String.format("CREATE CUSTOM INDEX ON %%s(%s) USING 'StorageAttachedIndex'", index));
            waitForIndexQueryable();
        }

        insertData(this, allRows, scenario);

        switch (scenario)
        {
            case SSTABLE_QUERY:
                flush();
                break;
            case COMPACTED_QUERY:
                flush();
                compact();
                break;
            case POST_BUILD_QUERY:
                flush();
                for (String index : dataset.decorateIndexColumn("value"))
                    createIndex(String.format("CREATE CUSTOM INDEX ON %%s(%s) USING 'StorageAttachedIndex'", index));
                waitForIndexQueryable();
                break;
        }

        dataset.querySet().runQueries(this, allRows);
    }

    public static void insertData(CQLTester tester, Object[][] allRows, Scenario scenario) throws Throwable
    {
        int sstableCounter = 0;
        int sstableIncrement = NUMBER_OF_VALUES / 8;
        for (int count = 0; count < allRows.length; count++)
        {
            tester.execute("INSERT INTO %s (pk, ck, value) VALUES (?, ?, ?)", allRows[count][0], allRows[count][1], allRows[count][2]);
            if ((scenario != Scenario.MEMTABLE_QUERY) && (++sstableCounter == sstableIncrement))
            {
                tester.flush();
                sstableCounter = 0;
            }
        }
    }

    public static Object[][] generateRows(DataSet<?> dataset, boolean widePartitions)
    {
        Object[][] allRows = new Object[dataset.values.length][];
        int partitionIncrement = NUMBER_OF_VALUES / 16;
        int partitionCounter = 0;
        int partition = 1;
        for (int index = 0; index < dataset.values.length; index++)
        {
            allRows[index] = row(partition, partitionCounter, dataset.values[index]);
            if (widePartitions)
            {
                if (++partitionCounter == partitionIncrement)
                {
                    partition++;
                    partitionCounter = 0;
                }
            }
            else
            {
                partition++;
            }
        }
        return allRows;
    }
}
