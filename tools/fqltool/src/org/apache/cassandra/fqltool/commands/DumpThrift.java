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

package org.apache.cassandra.fqltool.commands;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.zip.GZIPOutputStream;

import io.airlift.airline.Arguments;
import io.airlift.airline.Command;
import io.airlift.airline.Option;
import io.netty.buffer.Unpooled;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ChronicleQueueBuilder;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.RollCycles;
import net.openhft.chronicle.threads.Pauser;
import net.openhft.chronicle.wire.ReadMarshallable;
import net.openhft.chronicle.wire.ValueIn;
import org.antlr.runtime.RecognitionException;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.cql3.CQLFragmentParser;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.CqlParser;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.statements.DeleteStatement;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.cql3.statements.TruncateStatement;
import org.apache.cassandra.cql3.statements.UpdateStatement;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.fqltool.thrift.Batch;
import org.apache.cassandra.fqltool.thrift.BatchType;
import org.apache.cassandra.fqltool.thrift.LogEntry;
import org.apache.cassandra.fqltool.thrift.Query;
import org.apache.cassandra.fqltool.thrift.QueryOrBatch;
import org.apache.cassandra.fqltool.thrift.QueryStatement;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;

/**
 * Dump the contents of a list of paths containing full query logs
 */
@Command(name = "dump-thrift", description = "Dump the contents of a full query log")
public class DumpThrift implements Runnable
{
    @Arguments(usage = "<path1> [<path2>...<pathN>]", description = "Path containing the full query logs to dump.", required = true)
    private List<String> arguments = new ArrayList<>();

    @Option(title = "roll_cycle", name = { "--roll-cycle"}, description = "How often to roll the log file was rolled. May be necessary for Chronicle to correctly parse file names. (MINUTELY, HOURLY, DAILY). Default HOURLY.")
    private String rollCycle = "HOURLY";

    @Option(title = "follow", name = {"--follow"}, description = "Upon reacahing the end of the log continue indefinitely waiting for more records")
    private boolean follow = false;

    @Option(title = "output", name = {"--output", "--out"}, description = "Where to write the output to, if not defined will write to stdout")
    private String output = null;

    @Override
    public void run()
    {
        Config.setClientMode(true);

        if (output == null)
        {
            dump(arguments, rollCycle, follow, System.out::println);
            return;
        }
        try (DataOutputStream out = new DataOutputStream(new GZIPOutputStream(new FileOutputStream(output))))
        {
            TSerializer serializer = new TSerializer();
            class Counter { int value; }
            Counter counter = new Counter();
            Consumer<LogEntry> consumer = le -> {
                try
                {
                    byte[] bytes = serializer.serialize(le);
                    if (bytes.length == 0) {
                        return;
                    }
                    out.writeInt(bytes.length);
                    out.write(bytes);
                    counter.value++;
                }
                catch (TException | IOException e)
                {
                    throw new RuntimeException(e);
                }
            };
            dump(arguments, rollCycle, follow, consumer);
            out.writeInt(0);
            System.out.println("Wrote " + counter.value + " log entries");
        }
        catch (FileNotFoundException e)
        {
            throw new IllegalArgumentException("Unable to write to " + output + "; does the parent exist?", e);
        }
        catch (IOException e)
        {
            throw new UncheckedIOException(e);
        }
    }

    public static void dump(List<String> arguments, String rollCycle, boolean follow, Consumer<LogEntry> consumer)
    {
        ReadMarshallable reader = wireIn -> {
            LogEntry entry = new LogEntry();

            String type = wireIn.read("type").text();
            int protocolVersion = wireIn.read("protocol-version").int32();
            QueryOptions options = QueryOptions.codec.decode(Unpooled.wrappedBuffer(wireIn.read("query-options").bytesStore().toTemporaryDirectByteBuffer()),
                                                             ProtocolVersion.decode(protocolVersion, true));
            entry.setQueryOptions(toThrift(options));
            entry.setQueryTime(wireIn.read("query-time").int64());
            QueryOrBatch queryOrBatch = new QueryOrBatch();
            if (type.equals("single"))
            {
                String query = wireIn.read("query").text();
                List<ByteBuffer> values = options.getValues() != null ? options.getValues() : Collections.EMPTY_LIST;
                Query q = new Query();
                q.setOriginal(query);
                maybeClassifyAndRewriteOriginal(options, q);
                q.setValues(values);
                queryOrBatch.setQuery(q);
            }
            else
            {
                Batch batch = new Batch();
                batch.setType(BatchType.valueOf(wireIn.read("batch-type").text()));
                ValueIn in = wireIn.read("queries");
                int numQueries = in.int32();
                List<Query> queries = new ArrayList<>(numQueries);
                for (int ii = 0; ii < numQueries; ii++)
                {
                    Query query = new Query();
                    query.setOriginal(in.text());
                    maybeClassifyAndRewriteOriginal(options, query);
                    queries.add(query);
                }
                in = wireIn.read("values");
                int numValues = in.int32();
                for (int ii = 0; ii < numValues; ii++)
                {
                    List<ByteBuffer> values = new ArrayList<>();
                    int numSubValues = in.int32();
                    for (int zz = 0; zz < numSubValues; zz++)
                    {
                        values.add(ByteBuffer.wrap(in.bytes()));
                    }
                    queries.get(ii).setValues(values);
                }
            }
            entry.setQueryOrBatch(queryOrBatch);
            consumer.accept(entry);
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

    private static void maybeClassifyAndRewriteOriginal(QueryOptions options, Query query)
    {
        CQLStatement.Raw parsed;
        try
        {
            parsed = CQLFragmentParser.parseAnyUnhandled(CqlParser::query, query.original);
        }
        catch (RecognitionException e)
        {
            // ignore errors
            return;
        }
        classifyStatement(query, parsed);
    }

    private static QueryStatement classify(CQLStatement.Raw stmt)
    {
        // since this is based off type checks, the order is based off common access patterns; write more often than read;
        // this order does not affect correctness.
        if (stmt instanceof UpdateStatement.ParsedInsert || stmt instanceof UpdateStatement.ParsedInsertJson || stmt instanceof UpdateStatement.ParsedUpdate)
        {
            return QueryStatement.UPDATE;
        }
        else if (stmt instanceof SelectStatement.RawStatement)
        {
            return QueryStatement.SELECT;
        }
        else if (stmt instanceof DeleteStatement.Parsed)
        {
            return QueryStatement.DELETE;
        }
        else if (stmt instanceof TruncateStatement)
        {
            return QueryStatement.TRUNCATE;
        }
        else
        {
            return QueryStatement.DDL;
        }
    }

    private static void classifyStatement(Query query, CQLStatement.Raw parsed)
    {
        query.setQueryStatement(classify(parsed));
    }

    private static org.apache.cassandra.fqltool.thrift.QueryOptions toThrift(QueryOptions options)
    {
        org.apache.cassandra.fqltool.thrift.QueryOptions o = new org.apache.cassandra.fqltool.thrift.QueryOptions();
        o.setConsistency(toThrift(options.getConsistency()));
        o.setSerialConsistency(toThrift(options.getSerialConsistency()));
        options.getTimestamp().ifPresent(o::setTimestamp);
        o.setPageSize(options.getPageSize());
        return o;
    }

    private static org.apache.cassandra.fqltool.thrift.ConsistencyLevel toThrift(ConsistencyLevel consistency)
    {
        return org.apache.cassandra.fqltool.thrift.ConsistencyLevel.valueOf(consistency.name());
    }
}
