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

package org.apache.cassandra.transport;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.EncryptionOptions;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.transport.messages.StartupMessage;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class StartupMessageTest extends CQLTester
{

    @BeforeClass
    public static void setUp()
    {
        requireNetwork();
    }

    @Test
    public void checksumOptionValidation()
    {
        testConnection("crc32", false);
        testConnection("CRC32", false);
        testConnection("cRc32", false);
        testConnection("adler32", false);
        testConnection("ADLER32", false);
        testConnection("aDlEr32", false);
        testConnection("nonesuchtype", true);
        testConnection("", true);
        // special case of no option supplied
        testConnection(null, false);
    }

    private void testConnection(String checksumType, boolean expectProtocolError)
    {
        try (TestClient client = new TestClient(checksumType))
        {
            client.connect();
            if (expectProtocolError)
                fail("Expected a protocol exception");
        }
        catch (Exception e)
        {
            if (!expectProtocolError)
                fail("Did not expect any exception");

            // This is a bit ugly, but SimpleClient::execute throws RuntimeException if it receives any ErrorMessage
            String expected = String.format("org.apache.cassandra.transport.ProtocolException: " +
                                            "Requested checksum type %s is not known or supported " +
                                            "by this version of Cassandra", checksumType);
            assertEquals(expected, e.getMessage());
        }
    }

    static class TestClient extends SimpleClient
    {
        private final String checksumType;
        TestClient(String checksumType)
        {
            super(nativeAddr.getHostAddress(), nativePort, ProtocolVersion.V5, true, new EncryptionOptions());
            this.checksumType = checksumType;
        }

        void connect() throws IOException
        {
            establishConnection();
            Map<String, String> options = new HashMap<>();
            options.put(StartupMessage.CQL_VERSION, QueryProcessor.CQL_VERSION.toString());

            if (checksumType != null)
                options.put(StartupMessage.CHECKSUM, checksumType);

            execute(new StartupMessage(options));
        }
    }
}
