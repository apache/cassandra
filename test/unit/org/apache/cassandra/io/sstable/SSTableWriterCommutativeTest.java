/*
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * 
 */
package org.apache.cassandra.io.sstable;

import java.io.File;
import java.io.IOException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.cassandra.CleanupHelper;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.context.CounterContext;
import org.apache.cassandra.db.marshal.CounterColumnType;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.streaming.OperationType;
import org.apache.cassandra.utils.NodeId;
import org.junit.Test;
import org.apache.cassandra.utils.ByteBufferUtil;

public class SSTableWriterCommutativeTest extends CleanupHelper
{
    private static final CounterContext cc = new CounterContext();
    private static final CounterColumnType ctype = CounterColumnType.instance;

    @Test
    public void testStandardColumn() throws IOException, ExecutionException, InterruptedException, UnknownHostException
    {
        testRecoverAndOpenCommutative(false, false);
    }

    @Test
    public void testStandardColumnExceedMemoryLimit() throws IOException, ExecutionException, InterruptedException, UnknownHostException
    {
        testRecoverAndOpenCommutative(false, true);
    }


    @Test
    public void testSuperColumn() throws IOException, ExecutionException, InterruptedException, UnknownHostException
    {
        testRecoverAndOpenCommutative(true, false);
    }

    @Test
    public void testSuperColumnExceedMemoryLimit() throws IOException, ExecutionException, InterruptedException, UnknownHostException
    {
        testRecoverAndOpenCommutative(true, true);
    }

    /**
     * test recovery and opening of commutative columns
     * @param superColumns whether to test with super columns
     * @param forceExceedMemoryLimit if true, sets "in_memory_compaction_limit_in_mb" to 0 to force use of LazilyCompactedRow, otherwise, PreCompactedRow is used
     */
    public void testRecoverAndOpenCommutative(boolean superColumns, boolean forceExceedMemoryLimit) throws IOException, ExecutionException, InterruptedException, UnknownHostException
    {
        String keyspace = "Keyspace1";
        String cfname   = superColumns ? "SuperCounter1" : "Counter1";

        Map<String, ColumnFamily> entries = new HashMap<String, ColumnFamily>();
        Map<String, ColumnFamily> cleanedEntries = new HashMap<String, ColumnFamily>();

        ColumnFamily cf;
        ColumnFamily cfCleaned;
        CounterContext.ContextState state;
        IColumn column;
        IColumn columnCleaned;
        ByteBuffer superColumnName;

        // key: k
        cf = ColumnFamily.create(keyspace, cfname);
        cfCleaned = ColumnFamily.create(keyspace, cfname);
        state = CounterContext.ContextState.allocate(4, 1);
        state.writeElement(NodeId.fromInt(2), 9L, 3L, true);
        state.writeElement(NodeId.fromInt(4), 4L, 2L);
        state.writeElement(NodeId.fromInt(6), 3L, 3L);
        state.writeElement(NodeId.fromInt(8), 2L, 4L);
        column = new CounterColumn( ByteBufferUtil.bytes("x"), state.context, 0L);
        columnCleaned = new CounterColumn( ByteBufferUtil.bytes("x"), cc.clearAllDelta(state.context), 0L);

        if (superColumns)
        {
            superColumnName = ByteBufferUtil.bytes("TestSuperColumn1");
            column = superCounterColumnify(superColumnName, column);
            columnCleaned = superCounterColumnify(superColumnName, columnCleaned);
        }

        cf.addColumn(column);
        cfCleaned.addColumn(columnCleaned);

        state = CounterContext.ContextState.allocate(4, 1);
        state.writeElement(NodeId.fromInt(1), 7L, 12L);
        state.writeElement(NodeId.fromInt(2), 5L, 3L, true);
        state.writeElement(NodeId.fromInt(3), 2L, 33L);
        state.writeElement(NodeId.fromInt(9), 1L, 24L);
        column = new CounterColumn( ByteBufferUtil.bytes("y"), state.context, 0L);
        columnCleaned = new CounterColumn( ByteBufferUtil.bytes("y"), cc.clearAllDelta(state.context), 0L);

        if (superColumns)
        {
            superColumnName = ByteBufferUtil.bytes("TestSuperColumn2");
            column = superCounterColumnify(superColumnName, column);
            columnCleaned = superCounterColumnify(superColumnName, columnCleaned);
        }

        cf.addColumn(column);
        cfCleaned.addColumn(columnCleaned);

        entries.put("k", cf);
        cleanedEntries.put("k", cfCleaned);

        // key: l
        cf = ColumnFamily.create(keyspace, cfname);
        cfCleaned = ColumnFamily.create(keyspace, cfname);
        state = CounterContext.ContextState.allocate(4, 1);
        state.writeElement(NodeId.fromInt(2), 9L, 3L, true);
        state.writeElement(NodeId.fromInt(4), 4L, 2L);
        state.writeElement(NodeId.fromInt(6), 3L, 3L);
        state.writeElement(NodeId.fromInt(8), 2L, 4L);
        column = new CounterColumn( ByteBufferUtil.bytes("x"), state.context, 0L);
        columnCleaned = new CounterColumn( ByteBufferUtil.bytes("x"), cc.clearAllDelta(state.context), 0L);

        if (superColumns)
        {
            superColumnName = ByteBufferUtil.bytes("TestSuperColumn3");
            column = superCounterColumnify(superColumnName, column);
            columnCleaned = superCounterColumnify(superColumnName, columnCleaned);
        }

        cf.addColumn(column);
        cfCleaned.addColumn(columnCleaned);

        state = CounterContext.ContextState.allocate(3, 0);
        state.writeElement(NodeId.fromInt(1), 7L, 12L);
        state.writeElement(NodeId.fromInt(3), 2L, 33L);
        state.writeElement(NodeId.fromInt(9), 1L, 24L);
        column = new CounterColumn( ByteBufferUtil.bytes("y"), state.context, 0L);
        columnCleaned = new CounterColumn( ByteBufferUtil.bytes("y"), cc.clearAllDelta(state.context), 0L);

        if (superColumns)
        {
            superColumnName = ByteBufferUtil.bytes("TestSuperColumn4");
            column = superCounterColumnify(superColumnName, column);
            columnCleaned = superCounterColumnify(superColumnName, columnCleaned);
        }

        cf.addColumn(column);
        cfCleaned.addColumn(columnCleaned);

        entries.put("l", cf);
        cleanedEntries.put("l", cfCleaned);

        // write out unmodified CF
        SSTableReader orig = SSTableUtils.prepare().ks(keyspace).cf(cfname).generation(0).write(entries);
        long origMaxTimestamp = orig.getMaxTimestamp();

        // whack the index to trigger the recover
        FileUtils.deleteWithConfirm(orig.descriptor.filenameFor(Component.PRIMARY_INDEX));
        FileUtils.deleteWithConfirm(orig.descriptor.filenameFor(Component.FILTER));

        // set in_memory_compaction_limit_in_mb small to force use of LazilyCompactedRow, otherwise, PreCompactedRow is used
        DatabaseDescriptor.setInMemoryCompactionLimit(forceExceedMemoryLimit ? 0 : 256);

        // re-build inline
        SSTableReader rebuilt = CompactionManager.instance.submitSSTableBuild(
            orig.descriptor,
            OperationType.AES
            ).get();

        // ensure max timestamp is captured during rebuild
        assert rebuilt.getMaxTimestamp() == origMaxTimestamp;

        // write out cleaned CF
        SSTableReader cleaned = SSTableUtils.prepare().ks(keyspace).cf(cfname).generation(0).write(cleanedEntries);

        // verify
        RandomAccessReader origFile    = RandomAccessReader.open(new File(orig.descriptor.filenameFor(SSTable.COMPONENT_DATA)), 8 * 1024 * 1024);
        RandomAccessReader cleanedFile = RandomAccessReader.open(new File(cleaned.descriptor.filenameFor(SSTable.COMPONENT_DATA)), 8 * 1024 * 1024);

        while(origFile.getFilePointer() < origFile.length() && cleanedFile.getFilePointer() < cleanedFile.length())
        {
            assert origFile.readByte() == cleanedFile.readByte();
        }
        assert origFile.getFilePointer() == origFile.length();
        assert cleanedFile.getFilePointer() == cleanedFile.length();
    }

    private IColumn superCounterColumnify(ByteBuffer superColumnName, IColumn column)
    {
        SuperColumn superColumn = new SuperColumn(superColumnName, CounterColumnType.instance);
        superColumn.addColumn(column);
        return superColumn;
    }
}
