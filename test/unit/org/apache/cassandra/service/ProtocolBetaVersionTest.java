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
package org.apache.cassandra.service;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.*;
import org.apache.cassandra.cql3.*;
import org.apache.cassandra.transport.*;
import org.apache.cassandra.transport.messages.*;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.fail;

public class ProtocolBetaVersionTest extends CQLTester
{
    @BeforeClass
    public static void setUp()
    {
        requireNetwork();
        DatabaseDescriptor.setBatchSizeWarnThresholdInKB(1);
    }

    @Test
    public void testProtocolBetaVersion() throws Exception
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, v int)");

        try (SimpleClient client = new SimpleClient(nativeAddr.getHostAddress(), nativePort, Server.BETA_VERSION, true, new EncryptionOptions.ClientEncryptionOptions()))
        {
            client.connect(false);
            for (int i = 0; i < 10; i++)
            {
                QueryMessage query = new QueryMessage(String.format("INSERT INTO %s.%s (pk, v) VALUES (%s, %s)",
                                                                    KEYSPACE,
                                                                    currentTable(),
                                                                    i, i), QueryOptions.DEFAULT);
                client.execute(query);
            }

            QueryMessage query = new QueryMessage(String.format("SELECT * FROM %s.%s",
                                                                KEYSPACE,
                                                                currentTable()), QueryOptions.DEFAULT);
            ResultMessage.Rows resp = (ResultMessage.Rows) client.execute(query);
            assertEquals(10, resp.result.size());
        }
        catch (Exception e)
        {
            fail("No exceptions should've been thrown: " + e.getMessage());
        }
    }

    @Test
    public void unforcedProtocolVersionTest() throws Exception
    {
        try (SimpleClient client = new SimpleClient(nativeAddr.getHostAddress(), nativePort, Server.BETA_VERSION, false, new EncryptionOptions.ClientEncryptionOptions()))
        {
            client.connect(false);
            fail("Exception should have been thrown");
        }
        catch (Exception e)
        {
            assertEquals("Beta version of server used (5), but USE_BETA flag is not set",
                         e.getMessage());
        }
    }
}

