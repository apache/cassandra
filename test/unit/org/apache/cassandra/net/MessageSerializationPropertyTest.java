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
package org.apache.cassandra.net;

import java.io.Serializable;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadQuery;
import org.apache.cassandra.io.IVersionedAsymmetricSerializer;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.SchemaProvider;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.CassandraGenerators;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.FixedMonotonicClock;
import org.assertj.core.api.Assertions;
import org.mockito.Mockito;

import static org.apache.cassandra.config.CassandraRelevantProperties.CLOCK_MONOTONIC_APPROX;
import static org.apache.cassandra.config.CassandraRelevantProperties.CLOCK_MONOTONIC_PRECISE;
import static org.apache.cassandra.config.CassandraRelevantProperties.ORG_APACHE_CASSANDRA_DISABLE_MBEAN_REGISTRATION;
import static org.apache.cassandra.net.Message.serializer;
import static org.apache.cassandra.utils.CassandraGenerators.MESSAGE_GEN;
import static org.apache.cassandra.utils.FailingConsumer.orFail;
import static org.quicktheories.QuickTheory.qt;

public class MessageSerializationPropertyTest implements Serializable
{
    @BeforeClass
    public static void beforeClass()
    {
        ORG_APACHE_CASSANDRA_DISABLE_MBEAN_REGISTRATION.setBoolean(true);
        // message serialization uses the MonotonicClock class for precise and approx timestamps, so mock it out
        CLOCK_MONOTONIC_PRECISE.setString(FixedMonotonicClock.class.getName());
        CLOCK_MONOTONIC_APPROX.setString(FixedMonotonicClock.class.getName());

        DatabaseDescriptor.daemonInitialization();
    }

    /**
     * Validates that {@link Message#serializedSize(int)} == {@link Message.Serializer#serialize(Message, DataOutputPlus, int)} size.
     */
    @Test
    public void serializeSizeProperty()
    {
        try (DataOutputBuffer out = new DataOutputBuffer(1024))
        {
            qt().withShrinkCycles(0).forAll(MESSAGE_GEN).checkAssert(orFail(message -> {
                for (MessagingService.Version version : MessagingService.Version.supportedVersions())
                {
                    out.clear();
                    serializer.serialize(message, out, version.value);
                    Assertions.assertThat(out.getLength())
                              .as("Property serialize(out, version).length == serializedSize(version) " +
                                  "was violated for version %s and verb %s",
                                  version, message.header.verb)
                              .isEqualTo(message.serializedSize(version.value));
                }
            }));
        }
    }

    /**
     * Message and payload don't define equals, so have to rely on another way to define equality; serialized bytes!
     * The assumption is that serialize(deserialize(serialize(message))) == serialize(message)
     */
    @Test
    public void testMessageSerialization() throws Exception
    {
        SchemaProvider schema = Mockito.mock(SchemaProvider.class, Mockito.CALLS_REAL_METHODS);
        ReadCommand.Serializer readCommandSerializer = new ReadCommand.Serializer(schema);
        Supplier<? extends IVersionedAsymmetricSerializer<?, ?>> original = Verb.READ_REQ.unsafeSetSerializer(() -> readCommandSerializer);
        try (DataOutputBuffer first = new DataOutputBuffer(1024);
             DataOutputBuffer second = new DataOutputBuffer(1024))
        {
            qt().withShrinkCycles(0).forAll(MESSAGE_GEN).checkAssert(orFail(message -> {
                withTable(schema, message, orFail(ignore -> {
                    for (MessagingService.Version version : MessagingService.Version.supportedVersions())
                    {
                        first.clear();
                        second.clear();

                        // sync the clock with the generated createdAtNanos
                        FixedMonotonicClock.setNowInNanos(message.createdAtNanos());

                        serializer.serialize(message, first, version.value);
                        Message<Object> read = serializer.deserialize(new DataInputBuffer(first.buffer(), true), FBUtilities.getBroadcastAddressAndPort(), version.value);
                        serializer.serialize(read, second, version.value);
                        // using hex as byte buffer equality kept failing, and was harder to debug difference
                        // using hex means the specific section of the string that is different will be shown
                        Assertions.assertThat(ByteBufferUtil.bytesToHex(second.buffer()))
                                  .as("Property serialize(deserialize(serialize(message))) == serialize(message) "
                                      + "was violated for version %s and verb %s"
                                      + "\n first=%s"
                                      + "\nsecond=%s\n",
                                      version,
                                      message.header.verb,
                                      // toString methods are not relyable for messages, so use reflection to generate one
                                      new Object() { public String toString() { return CassandraGenerators.toStringRecursive(message); } },
                                      new Object() { public String toString() { return CassandraGenerators.toStringRecursive(read); } })
                                  .isEqualTo(ByteBufferUtil.bytesToHex(first.buffer()));
                    }
                }));
            }));
        }
        finally
        {
            Verb.READ_REQ.unsafeSetSerializer(original);
        }
    }

    private static void withTable(SchemaProvider schema, Message<?> message, Consumer<TableMetadata> fn)
    {
        TableMetadata metadata = null;
        if (message.payload instanceof ReadQuery)
            metadata = ((ReadQuery) message.payload).metadata();

        if (metadata != null)
            Mockito.when(schema.getTableMetadata(metadata.id)).thenReturn(metadata);

        fn.accept(metadata);

        if (metadata != null)
            Mockito.when(schema.getTableMetadata(metadata.id)).thenReturn(null);
    }
}
