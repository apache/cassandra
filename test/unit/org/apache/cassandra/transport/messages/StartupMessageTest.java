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

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.transport.Envelope;
import org.apache.cassandra.transport.Message;
import org.apache.cassandra.transport.ProtocolException;
import org.apache.cassandra.transport.ProtocolVersion;

public class StartupMessageTest extends EncodeAndDecodeTestBase<StartupMessage>
{
    static WithProperties properties;

    @BeforeClass
    public static void setUp()
    {
        DatabaseDescriptor.daemonInitialization();
        // Pin the limits to their current defaults so that the boundary tests below stay valid if the defaults change.
        properties = new WithProperties().set(CassandraRelevantProperties.STARTUP_CLIENT_OPTION_MAX_COUNT, 64)
                                         .set(CassandraRelevantProperties.STARTUP_CLIENT_OPTION_KEY_MAX_LENGTH, 256)
                                         .set(CassandraRelevantProperties.STARTUP_CLIENT_OPTION_VALUE_MAX_LENGTH, 1024);
    }

    @AfterClass
    public static void clearOptionLimits()
    {
        properties.close();
    }

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

    @Test
    public void testSupplementaryCodePointAllowed()
    {
        // U+1000A is a valid supplementary code point whose low 16 bits are 0x000A ('\n'); it must
        // not be truncated to a char and mistaken for a newline.
        String value = new String(Character.toChars(0x1000A));
        StartupMessage.validateOptions(Map.of(StartupMessage.CQL_VERSION, "3.0.0", "KEY", value));
    }

    @Test(expected = ProtocolException.class)
    public void testBidiOverrideFormatCharRejected()
    {
        // U+202E is a bidirectional text override formatting character (Right-to-Left Override)
        StartupMessage.validateOptions(Map.of("KEY", "value\u202Espoofed"));
    }

    @Test
    public void testNegativeLimitMeansUnlimited()
    {
        try (WithProperties ignored = new WithProperties().set(CassandraRelevantProperties.STARTUP_CLIENT_OPTION_VALUE_MAX_LENGTH, -1))
        {
            StartupMessage.validateOptions(Map.of(StartupMessage.CQL_VERSION, "3.0.0", "KEY", "b".repeat(5000)));
        }
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

    @Test
    public void testMaxEncodedSize()
    {
        long expected = Envelope.Header.LENGTH + 2L + 64L * (2L + 256 + 2L + 1024);
        Assert.assertEquals(expected, StartupMessage.maxEncodedSize());
    }

    @Test
    public void testMaxEncodedSizeWithZeroOptions()
    {
        try (WithProperties ignored = new WithProperties()
                                      .set(CassandraRelevantProperties.STARTUP_CLIENT_OPTION_MAX_COUNT, 0))
        {
            long expected = Envelope.Header.LENGTH + 2L;
            Assert.assertEquals(expected, StartupMessage.maxEncodedSize());
        }
    }

    @Test
    public void testMaxEncodedSizeWithUnlimitedCount()
    {
        try (WithProperties ignored = new WithProperties()
                                      .set(CassandraRelevantProperties.STARTUP_CLIENT_OPTION_MAX_COUNT, -1))
        {
            Assert.assertEquals(DatabaseDescriptor.getNativeTransportMaxFrameSize(), StartupMessage.maxEncodedSize());
        }
    }

    @Test
    public void testMaxEncodedSizeWithUnlimitedKeyLength()
    {
        try (WithProperties ignored = new WithProperties()
                                      .set(CassandraRelevantProperties.STARTUP_CLIENT_OPTION_KEY_MAX_LENGTH, -1))
        {
            Assert.assertEquals(DatabaseDescriptor.getNativeTransportMaxFrameSize(),StartupMessage.maxEncodedSize());
        }
    }

    @Test
    public void testMaxEncodedSizeWithUnlimitedValueLength()
    {
        try (WithProperties ignored = new WithProperties()
                                      .set(CassandraRelevantProperties.STARTUP_CLIENT_OPTION_VALUE_MAX_LENGTH, -1))
        {
            Assert.assertEquals(DatabaseDescriptor.getNativeTransportMaxFrameSize(), StartupMessage.maxEncodedSize());
        }
    }

    @Test
    public void testMaxEncodedSizeDoesNotExceedFrameLimit()
    {
        try (WithProperties ignored = new WithProperties()
                                      .set(CassandraRelevantProperties.STARTUP_CLIENT_OPTION_MAX_COUNT, Integer.MAX_VALUE)
                                      .set(CassandraRelevantProperties.STARTUP_CLIENT_OPTION_KEY_MAX_LENGTH, Integer.MAX_VALUE)
                                      .set(CassandraRelevantProperties.STARTUP_CLIENT_OPTION_VALUE_MAX_LENGTH, Integer.MAX_VALUE))
        {
            Assert.assertEquals(DatabaseDescriptor.getNativeTransportMaxFrameSize(), StartupMessage.maxEncodedSize());
        }
    }

    protected Message.Codec<StartupMessage> getCodec()
    {
        return StartupMessage.codec;
    }
}
