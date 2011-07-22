/**
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

package org.apache.cassandra.db;

import org.junit.Test;

import org.apache.cassandra.CleanupHelper;
import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.utils.WrappedRunnable;
import static org.apache.cassandra.Util.column;

import org.apache.cassandra.Util;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.utils.ByteBufferUtil;


public class LongTableTest extends CleanupHelper
{
    @Test
    public void testGetRowMultiColumn() throws Throwable
    {
        final Table table = Table.open("Keyspace1");
        final ColumnFamilyStore cfStore = table.getColumnFamilyStore("Standard1");

        for (int i = 1; i < 5000; i += 100)
        {
            RowMutation rm = new RowMutation("Keyspace1", Util.dk("key" + i).key);
            ColumnFamily cf = ColumnFamily.create("Keyspace1", "Standard1");
            for (int j = 0; j < i; j++)
                cf.addColumn(column("c" + j, "v" + j, 1L));
            rm.add(cf);
            rm.applyUnsafe();
        }

        Runnable verify = new WrappedRunnable()
        {
            public void runMayThrow() throws Exception
            {
                ColumnFamily cf;
                for (int i = 1; i < 5000; i += 100)
                {
                    for (int j = 0; j < i; j++)
                    {
                        cf = cfStore.getColumnFamily(QueryFilter.getNamesFilter(Util.dk("key" + i), new QueryPath("Standard1"), ByteBufferUtil.bytes("c" + j)));
                        TableTest.assertColumns(cf, "c" + j);
                    }
                }

            }
        };
        TableTest.reTest(table.getColumnFamilyStore("Standard1"), verify);
    }
}
