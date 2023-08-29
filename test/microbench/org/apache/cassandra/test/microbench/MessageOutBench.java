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

package org.apache.cassandra.test.microbench;

import java.io.IOException;
import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.google.common.net.InetAddresses;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.GossipDigest;
import org.apache.cassandra.gms.GossipDigestAck;
import org.apache.cassandra.gms.HeartBeatState;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.NoPayload;
import org.apache.cassandra.net.ParamType;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.utils.TimeUUID;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.apache.cassandra.net.Verb.ECHO_REQ;
import static org.apache.cassandra.net.Verb.GOSSIP_DIGEST_ACK;
import static org.apache.cassandra.utils.Clock.Global.nanoTime;
import static org.apache.cassandra.utils.TimeUUID.Generator.nextTimeUUID;

@State(Scope.Thread)
@Warmup(iterations = 4, time = 1)
@Measurement(iterations = 4, time = 4)
@Fork(value = 1, jvmArgsAppend = "-Xmx512M")
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@BenchmarkMode(Mode.SampleTime)
public class MessageOutBench
{
    @Param({ "true", "false" })
    private boolean withParams;

    @Param({ "GOSSIP_DIGEST_ACK", "ECHO_REQ" })
    private String verb;

    private Message msgOut;
    private ByteBuf buf;
    private InetAddressAndPort addr;

    @Setup
    public void setup()
    {
        DatabaseDescriptor.daemonInitialization();

        TimeUUID timeUuid = nextTimeUUID();
        Map<ParamType, Object> parameters = new EnumMap<>(ParamType.class);

        if (withParams)
        {
            parameters.put(ParamType.TRACE_SESSION, timeUuid);
        }

        addr = InetAddressAndPort.getByAddress(InetAddresses.forString("127.0.73.101"));
        msgOut = createMessage(verb, addr).withParams(parameters);
        buf = Unpooled.buffer(1024, 1024); // 1k should be enough for everybody!
    }

    private static Message<?> createMessage(String verb, InetAddressAndPort addr)
    {
        Verb verbEnum = Verb.valueOf(verb);

        switch (verbEnum)
        {
            case ECHO_REQ:
                return Message.builder(ECHO_REQ, NoPayload.noPayload).from(addr).build();
            case GOSSIP_DIGEST_ACK:
                HeartBeatState hb = new HeartBeatState(123, 456);
                GossipDigestAck payload = new GossipDigestAck(singletonList(new GossipDigest(addr, hb.getGeneration(), hb.getHeartBeatVersion())),
                        singletonMap(addr, new EndpointState(hb)));
                return Message.builder(GOSSIP_DIGEST_ACK, payload).from(addr).build();
            default:
                throw new IllegalArgumentException("Unknown verb: " + verb);
        }
    }

    @Benchmark
    public int serialize40() throws Exception
    {
        return serialize(MessagingService.VERSION_40);
    }

    @Benchmark
    public int serialize50() throws Exception
    {
        return serialize(MessagingService.VERSION_50);
    }

    @Benchmark
    public int serialize501() throws Exception
    {
        return serialize(MessagingService.VERSION_501);
    }

    private int serialize(int messagingVersion) throws IOException
    {
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            Message.serializer.serialize(Message.builder(msgOut).withCreatedAt(nanoTime()).withId(42).build(),
                                         out, messagingVersion);
            DataInputBuffer in = new DataInputBuffer(out.buffer(), false);
            Message.serializer.deserialize(in, addr, messagingVersion);
            return msgOut.serializedSize(messagingVersion);
        }
    }

    public static void main(String[] args) throws Exception
    {
        Options opt = new OptionsBuilder()
                .include(MessageOutBench.class.getSimpleName())
                .build();
        new Runner(opt).run();
    }
}
