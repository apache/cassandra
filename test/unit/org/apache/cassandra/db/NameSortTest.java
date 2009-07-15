/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.apache.cassandra.db;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.Collection;
import java.util.Arrays;

import org.junit.Test;

import org.apache.cassandra.CleanupHelper;
import org.apache.cassandra.db.filter.QueryPath;

public class NameSortTest extends CleanupHelper
{
    @Test
    public void testNameSort1() throws IOException, ExecutionException, InterruptedException
    {
        // single key
        testNameSort(1);
    }

    @Test
    public void testNameSort10() throws IOException, ExecutionException, InterruptedException
    {
        // multiple keys, flushing concurrently w/ inserts
        testNameSort(10);
    }

    @Test
    public void testNameSort100() throws IOException, ExecutionException, InterruptedException
    {
        // enough keys to force compaction concurrently w/ inserts
        testNameSort(100);
    }


    private void testNameSort(int N) throws IOException, ExecutionException, InterruptedException
    {
        Table table = Table.open("Table1");

        for (int i = 0; i < N; ++i)
        {
            String key = Integer.toString(i);
            RowMutation rm;

            // standard
            for (int j = 0; j < 8; ++j)
            {
                byte[] bytes = j % 2 == 0 ? "a".getBytes() : "b".getBytes();
                rm = new RowMutation("Table1", key);
                rm.add(new QueryPath("Standard1", null, "Column-" + j), bytes, j);
                rm.apply();
            }

            // super
            for (int j = 0; j < 8; ++j)
            {
                for (int k = 0; k < 4; ++k)
                {
                    byte[] bytes = (j + k) % 2 == 0 ? "a".getBytes() : "b".getBytes();
                    rm = new RowMutation("Table1", key);
                    rm.add(new QueryPath("Super1", "SuperColumn-" + j, "Column-" + k), bytes, k);
                    rm.apply();
                }
            }
        }

        validateNameSort(table, N);

        table.getColumnFamilyStore("Standard1").forceBlockingFlush();
        table.getColumnFamilyStore("Super1").forceBlockingFlush();
        validateNameSort(table, N);
    }

    private void validateNameSort(Table table, int N) throws IOException
    {
        for (int i = 0; i < N; ++i)
        {
            String key = Integer.toString(i);
            ColumnFamily cf;

            cf = table.get(key, "Standard1");
            Collection<IColumn> columns = cf.getAllColumns();
            for (IColumn column : columns)
            {
                int j = Integer.valueOf(column.name().split("-")[1]);
                byte[] bytes = j % 2 == 0 ? "a".getBytes() : "b".getBytes();
                assert Arrays.equals(bytes, column.value());
            }

            cf = table.get(key, "Super1");
            assert cf != null : "key " + key + " is missing!";
            Collection<IColumn> superColumns = cf.getAllColumns();
            assert superColumns.size() == 8;
            for (IColumn superColumn : superColumns)
            {
                int j = Integer.valueOf(superColumn.name().split("-")[1]);
                Collection<IColumn> subColumns = superColumn.getSubColumns();
                assert subColumns.size() == 4;
                for (IColumn subColumn : subColumns)
                {
                    int k = Integer.valueOf(subColumn.name().split("-")[1]);
                    byte[] bytes = (j + k) % 2 == 0 ? "a".getBytes() : "b".getBytes();
                    assert Arrays.equals(bytes, subColumn.value());
                }
            }
        }
    }

}
