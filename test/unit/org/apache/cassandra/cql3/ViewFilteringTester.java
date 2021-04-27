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
import java.util.stream.Collectors;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.datastax.driver.core.exceptions.OperationTimedOutException;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.transport.ProtocolVersion;

/* ViewComplexTest class has been split into multiple ones because of timeout issues (CASSANDRA-16670)
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
public abstract class ViewFilteringTester extends CQLTester
{
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
        System.setProperty("cassandra.mv.allow_filtering_nonkey_columns_unsafe", "true");
    }

    @AfterClass
    public static void tearDown()
    {
        System.setProperty("cassandra.mv.allow_filtering_nonkey_columns_unsafe", "false");
    }

    @Before
    public void begin()
    {
        views.clear();
    }

    @After
    public void end() throws Throwable
    {
        for (String viewName : views)
            executeNet(version, "DROP MATERIALIZED VIEW " + viewName);
    }

    protected void createView(String name, String query) throws Throwable
    {
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
    }

    protected void updateView(String query, Object... params) throws Throwable
    {
        executeNet(version, query, params);
        while (!(Stage.VIEW_MUTATION.executor().getPendingTaskCount() == 0
                 && Stage.VIEW_MUTATION.executor().getActiveTaskCount() == 0))
        {
            Thread.sleep(1);
        }
    }

    public void dropView(String name)
    {
        try
        {
            executeNet(version, "DROP MATERIALIZED VIEW " + name);
        }
        catch (Throwable e)
        {
            throw new RuntimeException(e);
        }
        views.remove(name);
    }

    protected static void waitForView(String keyspace, String view) throws InterruptedException
    {
        while (!SystemKeyspace.isViewBuilt(keyspace, view))
            Thread.sleep(10);
    }
}
