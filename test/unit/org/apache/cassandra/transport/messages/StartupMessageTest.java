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
        StartupMessage.validateOptions(options);
    }

    @Test(expected = ProtocolException.class)
    public void testKeyTooLong()
    {
        StartupMessage.validateOptions(Map.of("a".repeat(257), "value"));
    }

    @Test(expected = ProtocolException.class)
    public void testValueTooLong()
    {
        StartupMessage.validateOptions(Map.of("KEY", "b".repeat(1025)));
    }

    @Test(expected = ProtocolException.class)
    public void testControlCharacterInValue()
    {
        StartupMessage.validateOptions(Map.of("KEY", "value\u0001withcontrol"));
    }

    @Test(expected = ProtocolException.class)
    public void testNewlineRejectedInValue()
    {
        StartupMessage.validateOptions(Map.of("KEY", "value\nwithnewline"));
    }

    @Test
    public void testAllowedWhitespaceInValue()
    {
        StartupMessage.validateOptions(Map.of(StartupMessage.CQL_VERSION, "3.0.0", "KEY", "value\twith\ttab"));
    }

    @Test(expected = ProtocolException.class)
    public void testNullKeyThrowsException()
    {
        Map<String, String> options = new HashMap<>();
        options.put(null, "value");
        // Map.of() does not allow null keys, so we have to use a HashMap to test this case
        StartupMessage.validateOptions(options);
    }

    @Test
    public void testMultiByteUtf8Allowed()
    {
        StartupMessage.validateOptions(Map.of(StartupMessage.CQL_VERSION, "3.0.0", "DRIVER_NAME", "🚀-Cassandra-Driver-é"));
    }

    @Test(expected = ProtocolException.class)
    public void testBidiOverrideFormatCharRejected()
    {
        // U+202E is a bidirectional text override formatting character (Right-to-Left Override)
        StartupMessage.validateOptions(Map.of("KEY", "value\u202Espoofed"));
    }

    @Test
    public void testExactBoundariesAllowed()
    {
        Map<String, String> options = new HashMap<>();
        for (int i = 0; i < 63; i++)
        {
            options.put("KEY_" + i, "val");
        }
        options.put("a".repeat(256), "b".repeat(1024));

        StartupMessage.validateOptions(options);
    }

    protected Message.Codec<StartupMessage> getCodec()
    {
        return StartupMessage.codec;
    }
}
