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

package org.apache.cassandra.fqltool;


import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import net.openhft.chronicle.bytes.BytesStore;
import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.wire.ReadMarshallable;
import net.openhft.chronicle.wire.ValueIn;
import net.openhft.chronicle.wire.ValueOut;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireOut;
import net.openhft.chronicle.wire.WriteMarshallable;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.binlog.BinLog;

/**
 * note that we store each row as a separate chronicle document to be able to
 * avoid reading up the entire result set in memory when comparing
 *
 * document formats:
 * to mark the start of a new result set:
 * -------------------
 * version: int16
 * type: column_definitions
 * column_count: int32;
 * column_definition: text, text
 * column_definition: text, text
 * ....
 * --------------------
 *
 * to mark a failed query:
 * ---------------------
 * version: int16
 * type: query_failed
 * message: text
 * ---------------------
 *
 * row:
 * --------------------
 * version: int16
 * type: row
 * row_column_count: int32
 * column: bytes
 * ---------------------
 *
 * to mark the end of a result set:
 * -------------------
 * version: int16
 * type: end_resultset
 * -------------------
 *
 */
public class ResultStore
{
    private static final String VERSION = "version";
    private static final String TYPE = "type";
    // types:
    private static final String ROW = "row";
    private static final String END = "end_resultset";
    private static final String FAILURE = "query_failed";
    private static final String COLUMN_DEFINITIONS = "column_definitions";
    // fields:
    private static final String COLUMN_DEFINITION = "column_definition";
    private static final String COLUMN_COUNT = "column_count";
    private static final String MESSAGE = "message";
    private static final String ROW_COLUMN_COUNT = "row_column_count";
    private static final String COLUMN = "column";

    private static final int CURRENT_VERSION = 0;

    private final List<ChronicleQueue> queues;
    private final List<ExcerptAppender> appenders;
    private final ChronicleQueue queryStoreQueue;
    private final ExcerptAppender queryStoreAppender;
    private final Set<Integer> finishedHosts = new HashSet<>();

    public ResultStore(List<File> resultPaths, File queryFilePath)
    {
        queues = resultPaths.stream().map(path -> SingleChronicleQueueBuilder.single(path).build()).collect(Collectors.toList());
        appenders = queues.stream().map(ChronicleQueue::acquireAppender).collect(Collectors.toList());
        queryStoreQueue = queryFilePath != null ? SingleChronicleQueueBuilder.single(queryFilePath).build() : null;
        queryStoreAppender = queryStoreQueue != null ? queryStoreQueue.acquireAppender() : null;
    }

    /**
     * Store the column definitions in cds
     *
     * the ColumnDefinitions at position x will get stored by the appender at position x
     *
     * Calling this method indicates that we are starting a new result set from a query, it must be called before
     * calling storeRows.
     *
     */
    public void storeColumnDefinitions(FQLQuery query, List<ResultHandler.ComparableColumnDefinitions> cds)
    {
        finishedHosts.clear();
        if (queryStoreAppender != null)
        {
            BinLog.ReleaseableWriteMarshallable writeMarshallableQuery = query.toMarshallable();
            queryStoreAppender.writeDocument(writeMarshallableQuery);
            writeMarshallableQuery.release();
        }
        for (int i = 0; i < cds.size(); i++)
        {
            ResultHandler.ComparableColumnDefinitions cd = cds.get(i);
            appenders.get(i).writeDocument(new ColumnDefsWriter(cd));
        }
    }

    /**
     * Store rows
     *
     * the row at position x will get stored by appender at position x
     *
     * Before calling this for a new result set, storeColumnDefinitions must be called.
     */
    public void storeRows(List<ResultHandler.ComparableRow> rows)
    {
        for (int i = 0; i < rows.size(); i++)
        {
            ResultHandler.ComparableRow row = rows.get(i);
            if (row == null && !finishedHosts.contains(i))
            {
                appenders.get(i).writeDocument(wire -> {
                    wire.write(VERSION).int16(CURRENT_VERSION);
                    wire.write(TYPE).text(END);
                });
                finishedHosts.add(i);
            }
            else if (row != null)
            {
                appenders.get(i).writeDocument(new RowWriter(row));
            }
        }
    }

    public void close()
    {
        queues.forEach(Closeable::close);
        if (queryStoreQueue != null)
            queryStoreQueue.close();
    }

    static class ColumnDefsWriter implements WriteMarshallable
    {
        private final ResultHandler.ComparableColumnDefinitions defs;

        ColumnDefsWriter(ResultHandler.ComparableColumnDefinitions defs)
        {
            this.defs = defs;
        }

        public void writeMarshallable(WireOut wire)
        {
            wire.write(VERSION).int16(CURRENT_VERSION);
            if (!defs.wasFailed())
            {
                wire.write(TYPE).text(COLUMN_DEFINITIONS);
                wire.write(COLUMN_COUNT).int32(defs.size());
                for (ResultHandler.ComparableDefinition d : defs.asList())
                {
                    ValueOut vo = wire.write(COLUMN_DEFINITION);
                    vo.text(d.getName());
                    vo.text(d.getType());
                }
            }
            else
            {
                wire.write(TYPE).text(FAILURE);
                wire.write(MESSAGE).text(defs.getFailureException().getMessage());
            }
        }
    }

    static class ColumnDefsReader implements ReadMarshallable
    {
        boolean wasFailed;
        String failureMessage;
        List<Pair<String, String>> columnDefinitions = new ArrayList<>();

        public void readMarshallable(WireIn wire) throws IORuntimeException
        {
            int version = wire.read(VERSION).int16();
            String type = wire.read(TYPE).text();
            if (type.equals(FAILURE))
            {
                wasFailed = true;
                failureMessage = wire.read(MESSAGE).text();
            }
            else if (type.equals(COLUMN_DEFINITION))
            {
                int columnCount = wire.read(COLUMN_COUNT).int32();
                for (int i = 0; i < columnCount; i++)
                {
                    ValueIn vi = wire.read(COLUMN_DEFINITION);
                    String name = vi.text();
                    String dataType = vi.text();
                    columnDefinitions.add(Pair.create(name, dataType));
                }
            }
        }
    }

    /**
     * read a single row from the wire, or, marks itself finished if we read "end_resultset"
     */
    static class RowReader implements ReadMarshallable
    {
        boolean isFinished;
        List<ByteBuffer> rows = new ArrayList<>();

        public void readMarshallable(WireIn wire) throws IORuntimeException
        {
            int version = wire.read(VERSION).int32();
            String type = wire.read(TYPE).text();
            if (!type.equals(END))
            {
                isFinished = false;
                int rowColumnCount = wire.read(ROW_COLUMN_COUNT).int32();

                for (int i = 0; i < rowColumnCount; i++)
                {
                    byte[] b = wire.read(COLUMN).bytes();
                    rows.add(ByteBuffer.wrap(b));
                }
            }
            else
            {
                isFinished = true;
            }
        }
    }

    /**
     * Writes a single row to the given wire
     */
    static class RowWriter implements WriteMarshallable
    {
        private final ResultHandler.ComparableRow row;

        RowWriter(ResultHandler.ComparableRow row)
        {
            this.row = row;
        }

        public void writeMarshallable(WireOut wire)
        {
            wire.write(VERSION).int16(CURRENT_VERSION);
            wire.write(TYPE).text(ROW);
            wire.write(ROW_COLUMN_COUNT).int32(row.getColumnDefinitions().size());
            for (int jj = 0; jj < row.getColumnDefinitions().size(); jj++)
            {
                ByteBuffer bb = row.getBytesUnsafe(jj);
                if (bb != null)
                    wire.write(COLUMN).bytes(BytesStore.wrap(bb));
                else
                    wire.write(COLUMN).bytes("NULL".getBytes());
            }
        }
    }

}
