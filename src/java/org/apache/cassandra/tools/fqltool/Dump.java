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

package org.apache.cassandra.tools.fqltool;

import java.io.File;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import io.airlift.airline.Arguments;
import io.airlift.airline.Command;
import io.airlift.airline.Option;
import io.netty.buffer.Unpooled;
import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ChronicleQueueBuilder;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.RollCycles;
import net.openhft.chronicle.threads.Pauser;
import net.openhft.chronicle.wire.ReadMarshallable;
import net.openhft.chronicle.wire.ValueIn;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.transport.ProtocolVersion;

/**
 * Dump the contents of a list of paths containing full query logs
 */
@Command(name = "dump", description = "Dump the contents of a full query log")
public class Dump implements Runnable
{
    static final char[] HEXI_DECIMAL = "0123456789ABCDEF".toCharArray();

    @Arguments(usage = "<path1> [<path2>...<pathN>]", description = "Path containing the full query logs to dump.", required = true)
    private List<String> arguments = new ArrayList<>();

    @Option(title = "roll_cycle", name = {"--roll-cycle"}, description = "How often to roll the log file was rolled. May be necessary for Chronicle to correctly parse file names. (MINUTELY, HOURLY, DAILY). Default HOURLY.")
    private String rollCycle = "HOURLY";

    @Option(title = "follow", name = {"--follow"}, description = "Upon reacahing the end of the log continue indefinitely waiting for more records")
    private boolean follow = false;

    @Override
    public void run()
    {
        dump(arguments, rollCycle, follow);
    }

    public static void dump(List<String> arguments, String rollCycle, boolean follow)
    {
        StringBuilder sb = new StringBuilder();
        ReadMarshallable reader = wireIn -> {
            sb.setLength(0);
            String type = wireIn.read("type").text();
            sb.append("Type: ").append(type).append(System.lineSeparator());
            int protocolVersion = wireIn.read("protocol-version").int32();
            sb.append("Protocol version: ").append(protocolVersion).append(System.lineSeparator());
            QueryOptions options = QueryOptions.codec.decode(Unpooled.wrappedBuffer(wireIn.read("query-options").bytesStore().toTemporaryDirectByteBuffer()), ProtocolVersion.decode(protocolVersion));
            sb.append("Query time: ").append(wireIn.read("query-time").int64()).append(System.lineSeparator());
            if (type.equals("single"))
            {
                sb.append("Query: ").append(wireIn.read("query").text()).append(System.lineSeparator());
                List<ByteBuffer> values = options.getValues() != null ? options.getValues() : Collections.EMPTY_LIST;
                sb.append("Values: ").append(System.lineSeparator());
                valuesToStringBuilder(values, sb);
            }
            else
            {
                sb.append("Batch type: ").append(wireIn.read("batch-type").text()).append(System.lineSeparator());
                ValueIn in = wireIn.read("queries");
                int numQueries = in.int32();
                List<String> queries = new ArrayList<>();
                for (int ii = 0; ii < numQueries; ii++)
                {
                    queries.add(in.text());
                }
                in = wireIn.read("values");
                int numValues = in.int32();
                List<List<ByteBuffer>> values = new ArrayList<>();
                for (int ii = 0; ii < numValues; ii++)
                {
                    List<ByteBuffer> subValues = new ArrayList<>();
                    values.add(subValues);
                    int numSubValues = in.int32();
                    for (int zz = 0; zz < numSubValues; zz++)
                    {
                        subValues.add(ByteBuffer.wrap(in.bytes()));
                    }
                    sb.append("Query: ").append(queries.get(ii)).append(System.lineSeparator());
                    sb.append("Values: ").append(System.lineSeparator());
                    valuesToStringBuilder(subValues, sb);
                }
            }
            sb.append(System.lineSeparator());
            System.out.print(sb.toString());
            System.out.flush();
        };

        //Backoff strategy for spinning on the queue, not aggressive at all as this doesn't need to be low latency
        Pauser pauser = Pauser.millis(100);
        List<ChronicleQueue> queues = arguments.stream().distinct().map(path -> ChronicleQueueBuilder.single(new File(path)).rollCycle(RollCycles.valueOf(rollCycle)).build()).collect(Collectors.toList());
        List<ExcerptTailer> tailers = queues.stream().map(ChronicleQueue::createTailer).collect(Collectors.toList());
        boolean hadWork = true;
        while (hadWork)
        {
            hadWork = false;
            for (ExcerptTailer tailer : tailers)
            {
                while (tailer.readDocument(reader))
                {
                    hadWork = true;
                }
            }

            if (follow)
            {
                if (!hadWork)
                {
                    //Chronicle queue doesn't support blocking so use this backoff strategy
                    pauser.pause();
                }
                //Don't terminate the loop even if there wasn't work
                hadWork = true;
            }
        }
    }

    private static void valuesToStringBuilder(List<ByteBuffer> values, StringBuilder sb)
    {
        boolean first = true;
        for (ByteBuffer value : values)
        {
            Bytes bytes = Bytes.wrapForRead(value);
            long maxLength2 = Math.min(1024, bytes.readLimit() - bytes.readPosition());
            toHexString(bytes, bytes.readPosition(), maxLength2, sb);
            if (maxLength2 < bytes.readLimit() - bytes.readPosition())
            {
                sb.append("... truncated").append(System.lineSeparator());
            }

            if (first)
            {
                first = false;
            }
            else
            {
                sb.append("-----").append(System.lineSeparator());
            }
        }
    }

    //This is from net.openhft.chronicle.bytes, need to pass in the StringBuilder so had to copy
    /*
     * Copyright 2016 higherfrequencytrading.com
     *
     * Licensed under the Apache License, Version 2.0 (the "License");
     * you may not use this file except in compliance with the License.
     * You may obtain a copy of the License at
     *
     *     http://www.apache.org/licenses/LICENSE-2.0
     *
     * Unless required by applicable law or agreed to in writing, software
     * distributed under the License is distributed on an "AS IS" BASIS,
     * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
     * See the License for the specific language governing permissions and
     * limitations under the License.
     */
    /**
     * display the hex data of {@link Bytes} from the position() to the limit()
     *
     * @param bytes the buffer you wish to toString()
     * @return hex representation of the buffer, from example [0D ,OA, FF]
     */
    public static String toHexString(final Bytes bytes, long offset, long len, StringBuilder builder)
    throws BufferUnderflowException
    {
        if (len == 0)
            return "";

        int width = 16;
        int[] lastLine = new int[width];
        String sep = "";
        long position = bytes.readPosition();
        long limit = bytes.readLimit();

        try {
            bytes.readPositionRemaining(offset, len);

            long start = offset / width * width;
            long end = (offset + len + width - 1) / width * width;
            for (long i = start; i < end; i += width) {
                // check for duplicate rows
                if (i + width < end) {
                    boolean same = true;

                    for (int j = 0; j < width && i + j < offset + len; j++) {
                        int ch = bytes.readUnsignedByte(i + j);
                        same &= (ch == lastLine[j]);
                        lastLine[j] = ch;
                    }
                    if (i > start && same) {
                        sep = "........\n";
                        continue;
                    }
                }
                builder.append(sep);
                sep = "";
                String str = Long.toHexString(i);
                for (int j = str.length(); j < 8; j++)
                    builder.append('0');
                builder.append(str);
                for (int j = 0; j < width; j++) {
                    if (j == width / 2)
                        builder.append(' ');
                    if (i + j < offset || i + j >= offset + len) {
                        builder.append("   ");

                    } else {
                        builder.append(' ');
                        int ch = bytes.readUnsignedByte(i + j);
                        builder.append(HEXI_DECIMAL[ch >> 4]);
                        builder.append(HEXI_DECIMAL[ch & 15]);
                    }
                }
                builder.append(' ');
                for (int j = 0; j < width; j++) {
                    if (j == width / 2)
                        builder.append(' ');
                    if (i + j < offset || i + j >= offset + len) {
                        builder.append(' ');

                    } else {
                        int ch = bytes.readUnsignedByte(i + j);
                        if (ch < ' ' || ch > 126)
                            ch = '\u00B7';
                        builder.append((char) ch);
                    }
                }
                builder.append("\n");
            }
            return builder.toString();
        } finally {
            bytes.readLimit(limit);
            bytes.readPosition(position);
        }
    }
}
