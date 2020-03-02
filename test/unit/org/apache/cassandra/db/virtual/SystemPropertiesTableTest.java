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

package org.apache.cassandra.db.virtual;

import java.util.List;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import org.apache.cassandra.cql3.CQLTester;

public class SystemPropertiesTableTest extends CQLTester
{
    private static final String KS_NAME = "vts";

    private SystemPropertiesTable table;

    @BeforeClass
    public static void setUpClass()
    {
        CQLTester.setUpClass();
    }

    @Before
    public void config()
    {
        table = new SystemPropertiesTable(KS_NAME);
        VirtualKeyspaceRegistry.instance.register(new VirtualKeyspace(KS_NAME, ImmutableList.of(table)));
    }

    @Test
    public void testSelectAll() throws Throwable
    {
        ResultSet result = executeNet("SELECT * FROM vts.system_properties");

        for (Row r : result)
            Assert.assertEquals(System.getProperty(r.getString("name")), r.getString("value"));
    }

    @Test
    public void testSelectPartition() throws Throwable
    {
        List<String> properties = System.getProperties()
                                        .stringPropertyNames()
                                        .stream()
                                        .filter(name -> SystemPropertiesTable.isCassandraRelevant(name))
                                        .collect(Collectors.toList());

        for (String property : properties)
        {
            String q = "SELECT * FROM vts.system_properties WHERE name = '" + property + '\'';
            assertRowsNet(executeNet(q), new Object[] {property, System.getProperty(property)});
        }
    }

    @Test
    public void testSelectEmpty() throws Throwable
    {
        String q = "SELECT * FROM vts.system_properties WHERE name = 'EMPTY'";
        assertRowsNet(executeNet(q));
    }

}
