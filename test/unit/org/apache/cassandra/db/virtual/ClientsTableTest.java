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

import java.net.InetAddress;

import com.google.common.collect.ImmutableList;

import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import org.apache.cassandra.cql3.CQLTester;
import org.assertj.core.api.Assertions;
import static org.assertj.core.api.Assertions.assertThat;

public class ClientsTableTest extends CQLTester
{
    private static final String KS_NAME = "vts";
    
    private ClientsTable table;
    
    @BeforeClass
    public static void setUpClass()
    {
        CQLTester.setUpClass();
    }

    @Before
    public void config()
    {
        table = new ClientsTable(KS_NAME);
        VirtualKeyspaceRegistry.instance.register(new VirtualKeyspace(KS_NAME, ImmutableList.of(table)));
    }
    
    @Test
    public void testSelectAll() throws Throwable
    {
        ResultSet result = executeNet("SELECT * FROM vts.clients");
        
        for (Row r : result)
        {
            Assert.assertEquals(InetAddress.getLoopbackAddress(), r.getInet("address"));
            r.getInt("port");
            Assert.assertTrue(r.getInt("port") > 0);
            Assert.assertNotNull(r.getMap("client_options", String.class, String.class));
            Assert.assertTrue(r.getLong("request_count") > 0 );
            // the following are questionable if they belong here
            Assert.assertEquals("localhost", r.getString("hostname"));
            Assertions.assertThat(r.getMap("client_options", String.class, String.class))
                      .hasEntrySatisfying("DRIVER_VERSION", value -> assertThat(value.contains(r.getString("driver_name"))))
                      .hasEntrySatisfying("DRIVER_VERSION", value -> assertThat(value.contains(r.getString("driver_version"))));
        }
    }
}
