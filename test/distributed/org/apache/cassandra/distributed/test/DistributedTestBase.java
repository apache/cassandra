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

package org.apache.cassandra.distributed.test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import com.google.common.collect.Iterators;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;

import com.datastax.driver.core.ResultSet;
import org.apache.cassandra.distributed.impl.AbstractCluster;
import org.apache.cassandra.distributed.impl.IsolatedExecutor;
import org.apache.cassandra.distributed.impl.RowUtil;

public class DistributedTestBase
{
    @After
    public void afterEach()
    {
        System.runFinalization();
        System.gc();
    }

    public static String KEYSPACE = "distributed_test_keyspace";

    public static void nativeLibraryWorkaround()
    {
        // Disable the Netty tcnative library otherwise the io.netty.internal.tcnative.CertificateCallbackTask,
        // CertificateVerifierTask, SSLPrivateKeyMethodDecryptTask, SSLPrivateKeyMethodSignTask,
        // SSLPrivateKeyMethodTask, and SSLTask hold a gcroot against the InstanceClassLoader.
        System.setProperty("cassandra.disable_tcactive_openssl", "true");
        System.setProperty("io.netty.transport.noNative", "true");
    }

    public static void processReaperWorkaround()
    {
        // Make sure the 'process reaper' thread is initially created under the main classloader,
        // otherwise it gets created with the contextClassLoader pointing to an InstanceClassLoader
        // which prevents it from being garbage collected.
        IsolatedExecutor.ThrowingRunnable.toRunnable(() -> new ProcessBuilder().command("true").start().waitFor()).run();
    }

    @BeforeClass
    public static void setup()
    {
        System.setProperty("cassandra.ring_delay_ms", Integer.toString(10 * 1000));
        System.setProperty("org.apache.cassandra.disable_mbean_registration", "true");
        nativeLibraryWorkaround();
        processReaperWorkaround();
    }

    static String withKeyspace(String replaceIn)
    {
        return String.format(replaceIn, KEYSPACE);
    }

    protected static <C extends AbstractCluster<?>> C init(C cluster)
    {
        cluster.schemaChange("CREATE KEYSPACE " + KEYSPACE + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': " + cluster.size() + "};");
        return cluster;
    }

    public static void assertRows(ResultSet actual,Object[]... expected)
    {
        assertRows(RowUtil.toObjects(actual), expected);
    }

    public static void assertRows(Object[][] actual, Object[]... expected)
    {
        Assert.assertEquals(rowsNotEqualErrorMessage(actual, expected),
                            expected.length, actual.length);

        for (int i = 0; i < expected.length; i++)
        {
            Object[] expectedRow = expected[i];
            Object[] actualRow = actual[i];
            Assert.assertTrue(rowsNotEqualErrorMessage(actual, expected),
                              Arrays.equals(expectedRow, actualRow));
        }
    }

    public static void assertRow(Object[] actual, Object... expected)
    {
        Assert.assertTrue(rowNotEqualErrorMessage(actual, expected),
                          Arrays.equals(actual, expected));
    }

    public static void assertRows(Iterator<Object[]> actual, Iterator<Object[]> expected)
    {
        while (actual.hasNext() && expected.hasNext())
            assertRow(actual.next(), expected.next());

        Assert.assertEquals("Resultsets have different sizes", actual.hasNext(), expected.hasNext());
    }

    public static void assertRows(Iterator<Object[]> actual, Object[]... expected)
    {
        assertRows(actual, Iterators.forArray(expected));
    }

    public static String rowNotEqualErrorMessage(Object[] actual, Object[] expected)
    {
        return String.format("Expected: %s\nActual:%s\n",
                             Arrays.toString(expected),
                             Arrays.toString(actual));
    }

    public static String rowsNotEqualErrorMessage(Object[][] actual, Object[][] expected)
    {
        return String.format("Expected: %s\nActual: %s\n",
                             rowsToString(expected),
                             rowsToString(actual));
    }

    public static String rowsToString(Object[][] rows)
    {
        StringBuilder builder = new StringBuilder();
        builder.append("[");
        boolean isFirst = true;
        for (Object[] row : rows)
        {
            if (isFirst)
                isFirst = false;
            else
                builder.append(",");
            builder.append(Arrays.toString(row));
        }
        builder.append("]");
        return builder.toString();
    }

    public static Object[][] toObjectArray(Iterator<Object[]> iter)
    {
        List<Object[]> res = new ArrayList<>();
        while (iter.hasNext())
            res.add(iter.next());

        return res.toArray(new Object[res.size()][]);
    }

    public static Object[] row(Object... expected)
    {
        return expected;
    }
}
