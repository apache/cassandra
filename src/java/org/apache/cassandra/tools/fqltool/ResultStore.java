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
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import net.openhft.chronicle.bytes.BytesStore;
import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ChronicleQueueBuilder;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.wire.ValueOut;
import org.apache.cassandra.utils.binlog.BinLog;

/**
 * see FQLReplayTest#readResultFile for how to read files produced by this class
 */
public class ResultStore
{
    private final List<ChronicleQueue> queues;
    private final List<ExcerptAppender> appenders;
    private final ChronicleQueue queryStoreQueue;
    private final ExcerptAppender queryStoreAppender;
    private final Set<Integer> finishedHosts = new HashSet<>();

    public ResultStore(List<File> resultPaths, File queryFilePath)
    {
        queues = resultPaths.stream().map(path -> ChronicleQueueBuilder.single(path).build()).collect(Collectors.toList());
        appenders = queues.stream().map(ChronicleQueue::acquireAppender).collect(Collectors.toList());
        queryStoreQueue = queryFilePath != null ? ChronicleQueueBuilder.single(queryFilePath).build() : null;
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
            appenders.get(i).writeDocument(wire ->
                                           {
                                               if (!cd.wasFailed())
                                               {
                                                   wire.write("type").text("column_definitions");
                                                   wire.write("column_count").int32(cd.size());
                                                   for (ResultHandler.ComparableDefinition d : cd.asList())
                                                   {
                                                       ValueOut vo = wire.write("column_definition");
                                                       vo.text(d.getName());
                                                       vo.text(d.getType());
                                                   }
                                               }
                                               else
                                               {
                                                   wire.write("type").text("query_failed");
                                               }
                                           });
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
                appenders.get(i).writeDocument(wire -> wire.write("type").text("end_resultset"));
                finishedHosts.add(i);
            }
            else if (row != null)
            {
                appenders.get(i).writeDocument(wire ->
                                               {
                                                   {
                                                       wire.write("type").text("row");
                                                       wire.write("row_column_count").int32(row.getColumnDefinitions().size());
                                                       for (int jj = 0; jj < row.getColumnDefinitions().size(); jj++)
                                                       {
                                                           ByteBuffer bb = row.getBytesUnsafe(jj);
                                                           if (bb != null)
                                                               wire.write("column").bytes(BytesStore.wrap(bb));
                                                           else
                                                               wire.write("column").bytes("NULL".getBytes());
                                                       }
                                                   }
                                               });
            }
        }
    }

    public void close()
    {
        queues.forEach(Closeable::close);
        if (queryStoreQueue != null)
            queryStoreQueue.close();
    }
}
