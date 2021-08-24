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
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;

import com.datastax.driver.core.exceptions.OperationTimedOutException;
import org.apache.cassandra.concurrent.Stage;
import org.awaitility.Awaitility;

@Ignore
public abstract class ViewAbstractTest extends CQLTester
{
    protected final List<String> views = new ArrayList<>();

    @BeforeClass
    public static void startup()
    {
        requireNetwork();
    }

    @Before
    public void begin()
    {
        begin(views);
    }

    private static void begin(List<String> views)
    {
        views.clear();
    }

    @After
    public void end() throws Throwable
    {
        end(views, this);
    }

    private static void end(List<String> views, CQLTester tester) throws Throwable
    {
        for (String viewName : views)
            tester.executeNet("DROP MATERIALIZED VIEW " + viewName);
    }

    protected void createView(String name, String query) throws Throwable
    {
        createView(name, query, views, this);
    }

    private static void createView(String name, String query, List<String> views, CQLTester tester) throws Throwable
    {
        try
        {
            tester.executeNet(String.format(query, name));
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
        updateView(query, this, params);
    }

    private static void updateView(String query, CQLTester tester, Object... params) throws Throwable
    {
        tester.executeNet(query, params);
        waitForViewMutations();
    }

    protected static void waitForViewMutations()
    {
        Awaitility.await()
                  .atMost(5, TimeUnit.MINUTES)
                  .until(() -> Stage.VIEW_MUTATION.executor().getPendingTaskCount() == 0
                               && Stage.VIEW_MUTATION.executor().getActiveTaskCount() == 0);
    }
}
