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

package org.apache.cassandra.transport.messages;

import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.transport.Message;
import org.apache.cassandra.transport.ProtocolException;
import org.apache.cassandra.transport.ProtocolVersion;

public class StartupMessageTest extends EncodeAndDecodeTestBase<StartupMessage>
{
    @Test
    public void testEncodeAndDecode()
    {
        Map<String, String> options = new HashMap<>();
        options.put(StartupMessage.CQL_VERSION, "3.0.0");
        options.put(StartupMessage.DRIVER_NAME, "TestDriver");
        options.put(StartupMessage.DRIVER_VERSION, "1.0.0");

        StartupMessage origin = new StartupMessage(options);
        StartupMessage newMessage = encodeThenDecode(origin, ProtocolVersion.V5);
        Assert.assertEquals(origin.options, newMessage.options);
    }

    @Test(expected = ProtocolException.class)
    public void testTooManyEntries()
    {
        Map<String, String> options = new HashMap<>();
        for (int i = 0; i < 65; i++)
        {
            options.put("KEY_" + i, "value");
        }
        new StartupMessage(options);
    }

    @Test(expected = ProtocolException.class)
    public void testKeyTooLong()
    {
        Map<String, String> options = new HashMap<>();
        String longKey = new String(new char[257]).replace('\0', 'a');
        options.put(longKey, "value");
        new StartupMessage(options);
    }

    @Test(expected = ProtocolException.class)
    public void testValueTooLong()
    {
        Map<String, String> options = new HashMap<>();
        String longValue = new String(new char[1025]).replace('\0', 'b');
        options.put("KEY", longValue);
        new StartupMessage(options);
    }

    @Test(expected = ProtocolException.class)
    public void testControlCharacterInValue()
    {
        Map<String, String> options = new HashMap<>();
        options.put("KEY", "value\u0001withcontrol");
        new StartupMessage(options);
    }

    @Test
    public void testAllowedWhitespaceInValue()
    {
        Map<String, String> options = new HashMap<>();
        options.put(StartupMessage.CQL_VERSION, "3.0.0");
        options.put("KEY", "value\twith\nwhitespace\r");
        StartupMessage msg = new StartupMessage(options);
        Assert.assertNotNull(msg);
    }

    protected Message.Codec<StartupMessage> getCodec()
    {
        return StartupMessage.codec;
    }
}