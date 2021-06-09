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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.datastax.driver.core.exceptions.OperationTimedOutException;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.transport.ProtocolVersion;

import static org.apache.cassandra.db.ColumnFamilyStore.FlushReason.UNIT_TESTS;

/* ViewComplexTest class has been split into multiple ones because of timeout issues (CASSANDRA-16670, CASSANDRA-17167)
 * Any changes here check if they apply to the other classes:
 * - ViewComplexUpdatesTest
 * - ViewComplexDeletionsTest
 * - ViewComplexTTLTest
 * - ViewComplexTest
 * - ViewComplexLivenessTest
 * - ...
 * - ViewComplex*Test
 */
@RunWith(Parameterized.class)
public abstract class ViewComplexTester extends CQLTester
{
    private static final AtomicInteger seqNumber = new AtomicInteger();

    @Parameterized.Parameter
    public ProtocolVersion version;

    @Parameterized.Parameters()
    public static Collection<Object[]> versions()
    {
        return ProtocolVersion.SUPPORTED.stream()
                                        .map(v -> new Object[]{v})
                                        .collect(Collectors.toList());
    }

    protected final List<String> views = new ArrayList<>();

    @BeforeClass
    public static void startup()
    {
        requireNetwork();
    }

    @Before
    public void begin() throws Throwable
    {
        views.clear();
    }

    @After
    public void end() throws Throwable
    {
        dropMViews();
    }

    protected void dropMViews() throws Throwable
    {
        for (String viewName : views)
            executeNet(version, "DROP MATERIALIZED VIEW " + viewName);
    }

    protected String createView(String query) throws Throwable
    {
        String name = createViewName();

        try
        {
            executeNet(version, String.format(query, name));
            // If exception is thrown, the view will not be added to the list; since it shouldn't have been created, this is
            // the desired behavior
            views.add(name);
        }
        catch (OperationTimedOutException ex)
        {
            // ... except for timeout, when we actually do not know whether the view was created or not
            views.add(name);
            throw ex;
        }

        return name;
    }

    protected static String createViewName()
    {
        return "mv" + seqNumber.getAndIncrement();
    }

    protected void updateView(String query, Object... params) throws Throwable
    {
        updateViewWithFlush(query, false, params);
    }

    protected void updateViewWithFlush(String query, boolean flush, Object... params) throws Throwable
    {
        executeNet(version, query, params);
        while (!(Stage.VIEW_MUTATION.executor().getPendingTaskCount() == 0
                 && Stage.VIEW_MUTATION.executor().getActiveTaskCount() == 0))
        {
            Thread.sleep(1);
        }
        if (flush)
            Keyspace.open(keyspace()).flush(UNIT_TESTS);
    }
}
