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

package org.apache.cassandra.cql3;

import java.util.Random;

import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.ByteOrderedPartitioner;
import org.apache.cassandra.distributed.shared.ThrowingRunnable;
import org.apache.cassandra.service.StorageService;

import static org.apache.cassandra.cql3.TombstonesWithIndexedSSTableTest.makeRandomString;
import static org.junit.Assert.assertEquals;

/**
 * Tests primary index for prefixes. Exercises paths in the BTI partition iterators that are very hard to reach using
 * a hashing partitioner.
 */
public class DecoratedKeyPrefixesTest extends CQLTester
{
    // to force indexing, we need more than 2*column_index_size (4k for tests) bytes per partition.
    static final int ENTRY_SIZE = 100;
    static final int WIDE_ROW_COUNT = 100;

    String[] keys;
    String[] samePrefixBefore;
    String[] samePrefixAfter;

    Random rand = new Random();

    // Must be called the same as CQLTester's version to ensure it overrides it
    @BeforeClass
    public static void setUpClass()
    {
        DatabaseDescriptor.daemonInitialization();
        StorageService.instance.setPartitionerUnsafe(ByteOrderedPartitioner.instance);

        // Once per-JVM is enough
        prepareServer();
    }

    boolean wide(int i)
    {
        return i % 2 == 1;
    }

    private int randomCKey(int i)
    {
        return wide(i) ? rand.nextInt(WIDE_ROW_COUNT) : 0;
    }

    void prepareTable() throws Throwable
    {
        Assume.assumeTrue(DatabaseDescriptor.getPartitioner() instanceof ByteOrderedPartitioner);

        createTable("CREATE TABLE %s (pk text, ck int, v1 int, v2 text, primary key(pk, ck));");

        String[] baseKeys = new String[] {"A", "BB", "CCC", "DDDD"};
        keys = new String[baseKeys.length];
        samePrefixBefore = new String[baseKeys.length];
        samePrefixAfter = new String[baseKeys.length];

        // Create keys and bounds before and after it with the same prefix
        for (int i = 0; i < baseKeys.length; ++i)
        {
            String key = baseKeys[i];
            keys[i] = key + "key";
            samePrefixBefore[i] = key + "before";
            samePrefixAfter[i] = key + "larger";
            addPartition(keys[i], wide(i) ? WIDE_ROW_COUNT : 1);
        }
    }

    private void addPartition(String key, int rowCount) throws Throwable
    {
        for (int i = 0; i < rowCount; ++i)
            execute("INSERT INTO %s (pk, ck, v1, v2) values (?, ?, ?, ?)", key, i, 1, makeRandomString(ENTRY_SIZE));
    }

    void testWithFlush(ThrowingRunnable test) throws Throwable
    {
        prepareTable();
        test.run();
        flush();
        test.run();
    }

    @Test
    public void testPrefixLookupEQ() throws Throwable
    {
        testWithFlush(() ->
        {
            for (int i = 0; i < keys.length; ++i)
            {
                assertRows(execute("SELECT pk FROM %s WHERE pk = ? AND ck = ?", keys[i], randomCKey(i)),
                           row(keys[i]));
                assertEmpty(execute("SELECT pk FROM %s WHERE pk = ?", samePrefixBefore[i]));
                assertEmpty(execute("SELECT pk FROM %s WHERE pk = ?", samePrefixAfter[i]));
            }
        });
    }

    @Test
    public void testPrefixLookupGE() throws Throwable
    {
        testWithFlush(() ->
        {
            for (int i = 0; i < keys.length; ++i)
            {
                assertRows(execute("SELECT pk FROM %s WHERE token(pk) >= token(?) LIMIT 1", keys[i]),
                           row(keys[i]));
                assertRows(execute("SELECT pk FROM %s WHERE token(pk) >= token(?) LIMIT 1", samePrefixBefore[i]),
                           row(keys[i]));
                assertReturnsNext(execute("SELECT pk FROM %s WHERE token(pk) >= token(?) LIMIT 1", samePrefixAfter[i]), i);
            }
        });
    }

    void assertReturnsNext(UntypedResultSet result, int i) throws Throwable
    {
        if (i == keys.length - 1)
            assertEmpty(result);
        else
            assertRows(result, row(keys[i + 1]));
    }

    @Test
    public void testPrefixLookupGT() throws Throwable
    {
        testWithFlush(() ->
        {
            for (int i = 0; i < keys.length; ++i)
            {
                assertReturnsNext(execute("SELECT pk FROM %s WHERE token(pk) > token(?) LIMIT 1", keys[i]), i);
                assertRows(execute("SELECT pk FROM %s WHERE token(pk) > token(?) LIMIT 1", samePrefixBefore[i]),
                           row(keys[i]));
                assertReturnsNext(execute("SELECT pk FROM %s WHERE token(pk) > token(?) LIMIT 1", samePrefixAfter[i]), i);
            }
        });
    }

    @Test
    public void testPrefixKeySliceExact() throws Throwable
    {
        testWithFlush(() ->
        {
            for (int i = 0; i < keys.length; ++i)
            {
                for (int j = i; j < keys.length; ++j)
                {
                    assertEquals(i + "<>" + j, Math.max(0, j - i - 1),
                                 execute("SELECT pk FROM %s WHERE token(pk) > token(?) AND token(pk) < token(?) AND ck = 0 ALLOW FILTERING", keys[i], keys[j]).size());
                    assertEquals(i + "=<>" + j, j - i,
                                 execute("SELECT pk FROM %s WHERE token(pk) > token(?) AND token(pk) <= token(?) AND ck = 0 ALLOW FILTERING", keys[i], keys[j]).size());
                    assertEquals(i + "<>=" + j, j - i,
                                 execute("SELECT pk FROM %s WHERE token(pk) >= token(?) AND token(pk) < token(?) AND ck = 0 ALLOW FILTERING", keys[i], keys[j]).size());
                    assertEquals(i + "=<>=" + j, j - i + 1,
                                 execute("SELECT pk FROM %s WHERE token(pk) >= token(?) AND token(pk) <= token(?) AND ck = 0 ALLOW FILTERING", keys[i], keys[j]).size());
                }
            }
        });
    }

    @Test
    public void testPrefixKeySliceSamePrefix() throws Throwable
    {
        testWithFlush(() ->
                      {
                          for (int i = 0; i < keys.length; ++i)
                          {
                              for (int j = i; j < keys.length; ++j)
                              {
                                  assertEquals(i + "<ba>" + j, j - i + 1,
                                               execute("SELECT pk FROM %s WHERE token(pk) > token(?) AND token(pk) < token(?) AND ck = 0 ALLOW FILTERING", samePrefixBefore[i], samePrefixAfter[j]).size());
                                  assertEquals(i + "=<ba>" + j, j - i + 1,
                                               execute("SELECT pk FROM %s WHERE token(pk) > token(?) AND token(pk) <= token(?) AND ck = 0 ALLOW FILTERING", samePrefixBefore[i], samePrefixAfter[j]).size());
                                  assertEquals(i + "<ba>=" + j, j - i + 1,
                                               execute("SELECT pk FROM %s WHERE token(pk) >= token(?) AND token(pk) < token(?) AND ck = 0 ALLOW FILTERING", samePrefixBefore[i], samePrefixAfter[j]).size());
                                  assertEquals(i + "=<ba>=" + j, j - i + 1,
                                               execute("SELECT pk FROM %s WHERE token(pk) >= token(?) AND token(pk) <= token(?) AND ck = 0 ALLOW FILTERING", samePrefixBefore[i], samePrefixAfter[j]).size());

                                  assertEquals(i + "<aa>" + j, j - i,
                                               execute("SELECT pk FROM %s WHERE token(pk) > token(?) AND token(pk) < token(?) AND ck = 0 ALLOW FILTERING", samePrefixAfter[i], samePrefixAfter[j]).size());
                                  assertEquals(i + "=<aa>" + j, j - i,
                                               execute("SELECT pk FROM %s WHERE token(pk) > token(?) AND token(pk) <= token(?) AND ck = 0 ALLOW FILTERING", samePrefixAfter[i], samePrefixAfter[j]).size());
                                  assertEquals(i + "<aa>=" + j, j - i,
                                               execute("SELECT pk FROM %s WHERE token(pk) >= token(?) AND token(pk) < token(?) AND ck = 0 ALLOW FILTERING", samePrefixAfter[i], samePrefixAfter[j]).size());
                                  assertEquals(i + "=<aa>=" + j, j - i,
                                               execute("SELECT pk FROM %s WHERE token(pk) >= token(?) AND token(pk) <= token(?) AND ck = 0 ALLOW FILTERING", samePrefixAfter[i], samePrefixAfter[j]).size());

                                  assertEquals(i + "<bb>" + j, j - i,
                                               execute("SELECT pk FROM %s WHERE token(pk) > token(?) AND token(pk) < token(?) AND ck = 0 ALLOW FILTERING", samePrefixBefore[i], samePrefixBefore[j]).size());
                                  assertEquals(i + "=<bb>" + j, j - i,
                                               execute("SELECT pk FROM %s WHERE token(pk) > token(?) AND token(pk) <= token(?) AND ck = 0 ALLOW FILTERING", samePrefixBefore[i], samePrefixBefore[j]).size());
                                  assertEquals(i + "<bb>=" + j, j - i,
                                               execute("SELECT pk FROM %s WHERE token(pk) >= token(?) AND token(pk) < token(?) AND ck = 0 ALLOW FILTERING", samePrefixBefore[i], samePrefixBefore[j]).size());
                                  assertEquals(i + "=<bb>=" + j, j - i,
                                               execute("SELECT pk FROM %s WHERE token(pk) >= token(?) AND token(pk) <= token(?) AND ck = 0 ALLOW FILTERING", samePrefixBefore[i], samePrefixBefore[j]).size());

                                  assertEquals(i + "<ab>" + j, Math.max(0, j - i - 1),
                                               execute("SELECT pk FROM %s WHERE token(pk) > token(?) AND token(pk) < token(?) AND ck = 0 ALLOW FILTERING", samePrefixAfter[i], samePrefixBefore[j]).size());
                                  assertEquals(i + "=<ab>" + j, Math.max(0, j - i - 1),
                                               execute("SELECT pk FROM %s WHERE token(pk) > token(?) AND token(pk) <= token(?) AND ck = 0 ALLOW FILTERING", samePrefixAfter[i], samePrefixBefore[j]).size());
                                  assertEquals(i + "<ab>=" + j, Math.max(0, j - i - 1),
                                               execute("SELECT pk FROM %s WHERE token(pk) >= token(?) AND token(pk) < token(?) AND ck = 0 ALLOW FILTERING", samePrefixAfter[i], samePrefixBefore[j]).size());
                                  assertEquals(i + "=<ab>=" + j, Math.max(0, j - i - 1),
                                               execute("SELECT pk FROM %s WHERE token(pk) >= token(?) AND token(pk) <= token(?) AND ck = 0 ALLOW FILTERING", samePrefixAfter[i], samePrefixBefore[j]).size());
                              }
                          }
                      });
    }

    @Test
    public void testPrefixKeySliceMixed() throws Throwable
    {
        testWithFlush(() ->
                      {
                          for (int i = 0; i < keys.length; ++i)
                          {
                              for (int j = i; j < keys.length; ++j)
                              {
                                  assertEquals(i + "<bk>" + j, j - i,
                                               execute("SELECT pk FROM %s WHERE token(pk) > token(?) AND token(pk) < token(?) AND ck = 0 ALLOW FILTERING", samePrefixBefore[i], keys[j]).size());
                                  assertEquals(i + "=<bk>" + j, j - i + 1,
                                               execute("SELECT pk FROM %s WHERE token(pk) > token(?) AND token(pk) <= token(?) AND ck = 0 ALLOW FILTERING", samePrefixBefore[i], keys[j]).size());

                                  assertEquals(i + "<ak>" + j, Math.max(0, j - i - 1),
                                               execute("SELECT pk FROM %s WHERE token(pk) > token(?) AND token(pk) < token(?) AND ck = 0 ALLOW FILTERING", samePrefixAfter[i], keys[j]).size());
                                  assertEquals(i + "=<ak>" + j, j - i,
                                               execute("SELECT pk FROM %s WHERE token(pk) > token(?) AND token(pk) <= token(?) AND ck = 0 ALLOW FILTERING", samePrefixAfter[i], keys[j]).size());

                                  assertEquals(i + "<kb>" + j, Math.max(0, j - i - 1),
                                               execute("SELECT pk FROM %s WHERE token(pk) > token(?) AND token(pk) < token(?) AND ck = 0 ALLOW FILTERING", keys[i], samePrefixBefore[j]).size());
                                  assertEquals(i + "=<kb>" + j, j - i,
                                               execute("SELECT pk FROM %s WHERE token(pk) >= token(?) AND token(pk) < token(?) AND ck = 0 ALLOW FILTERING", keys[i], samePrefixBefore[j]).size());

                                  assertEquals(i + "<ka>" + j, j - i,
                                               execute("SELECT pk FROM %s WHERE token(pk) > token(?) AND token(pk) < token(?) AND ck = 0 ALLOW FILTERING", keys[i], samePrefixAfter[j]).size());
                                  assertEquals(i + "=<ka>" + j, j - i + 1,
                                               execute("SELECT pk FROM %s WHERE token(pk) >= token(?) AND token(pk) < token(?) AND ck = 0 ALLOW FILTERING", keys[i], samePrefixAfter[j]).size());
                              }
                          }
                      });
    }
}
