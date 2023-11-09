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

import java.util.Collection;
import java.util.stream.Collectors;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.datastax.driver.core.ResultSet;
import org.apache.cassandra.transport.ProtocolVersion;

@Ignore
@RunWith(Parameterized.class)
public abstract class ViewAbstractParameterizedTest extends ViewAbstractTest
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

    @Before
    @Override
    public void beforeTest() throws Throwable
    {
        super.beforeTest();

        executeNet("USE " + keyspace());
    }

    @Override
    protected com.datastax.driver.core.ResultSet executeNet(String query, Object... values)
    {
        return executeNet(version, query, values);
    }

    @Override
    protected com.datastax.driver.core.ResultSet executeNetWithPaging(String query, int pageSize, Object... values)
    {
        return executeNetWithPaging(version, query, pageSize, values);
    }

    @Override
    protected void assertRowsNet(ResultSet result, Object[]... rows)
    {
        assertRowsNet(version, result, rows);
    }

    @Override
    protected void updateView(String query, Object... params) throws Throwable
    {
        updateView(version, query, params);
    }

    protected void updateViewWithFlush(String query, boolean flush, Object... params) throws Throwable
    {
        updateView(query, params);
        if (flush)
            flush(keyspace());
    }
}
