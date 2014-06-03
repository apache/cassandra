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
package org.apache.cassandra.db;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.UUID;

import com.google.common.collect.Iterables;

import org.apache.cassandra.db.filter.NamesQueryFilter;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.HeapAllocator;

public class CounterMutation implements IMutation
{
    public static final CounterMutationSerializer serializer = new CounterMutationSerializer();

    private final RowMutation rowMutation;
    private final ConsistencyLevel consistency;

    public CounterMutation(RowMutation rowMutation, ConsistencyLevel consistency)
    {
        this.rowMutation = rowMutation;
        this.consistency = consistency;
    }

    public String getKeyspaceName()
    {
        return rowMutation.getKeyspaceName();
    }

    public Collection<UUID> getColumnFamilyIds()
    {
        return rowMutation.getColumnFamilyIds();
    }

    public Collection<ColumnFamily> getColumnFamilies()
    {
        return rowMutation.getColumnFamilies();
    }

    public ByteBuffer key()
    {
        return rowMutation.key();
    }

    public RowMutation rowMutation()
    {
        return rowMutation;
    }

    public ConsistencyLevel consistency()
    {
        return consistency;
    }

    public RowMutation makeReplicationMutation()
    {
        List<ReadCommand> readCommands = new LinkedList<ReadCommand>();
        long timestamp = System.currentTimeMillis();
        for (ColumnFamily columnFamily : rowMutation.getColumnFamilies())
        {
            if (!columnFamily.metadata().getReplicateOnWrite())
                continue;
            addReadCommandFromColumnFamily(rowMutation.getKeyspaceName(), rowMutation.key(), columnFamily, timestamp, readCommands);
        }

        // create a replication RowMutation
        RowMutation replicationMutation = new RowMutation(rowMutation.getKeyspaceName(), rowMutation.key());
        for (ReadCommand readCommand : readCommands)
        {
            Keyspace keyspace = Keyspace.open(readCommand.ksName);
            Row row = readCommand.getRow(keyspace);
            if (row == null || row.cf == null)
                continue;

            ColumnFamily cf = row.cf;
            replicationMutation.add(cf);
        }

        return replicationMutation.isEmpty() ? null : replicationMutation;
    }

    private void addReadCommandFromColumnFamily(String keyspaceName, ByteBuffer key, ColumnFamily columnFamily, long timestamp, List<ReadCommand> commands)
    {
        SortedSet<ByteBuffer> s = new TreeSet<ByteBuffer>(columnFamily.metadata().comparator);
        Iterables.addAll(s, columnFamily.getColumnNames());
        commands.add(new SliceByNamesReadCommand(keyspaceName, key, columnFamily.metadata().cfName, timestamp, new NamesQueryFilter(s)));
    }

    public MessageOut<CounterMutation> makeMutationMessage()
    {
        return new MessageOut<CounterMutation>(MessagingService.Verb.COUNTER_MUTATION, this, serializer);
    }

    public boolean shouldReplicateOnWrite()
    {
        for (ColumnFamily cf : rowMutation.getColumnFamilies())
            if (cf.metadata().getReplicateOnWrite())
                return true;
        return false;
    }

    public void apply()
    {
        // transform all CounterUpdateColumn to CounterColumn: accomplished by localCopy
        RowMutation rm = new RowMutation(rowMutation.getKeyspaceName(), ByteBufferUtil.clone(rowMutation.key()));
        Keyspace keyspace = Keyspace.open(rm.getKeyspaceName());

        for (ColumnFamily cf_ : rowMutation.getColumnFamilies())
        {
            ColumnFamily cf = cf_.cloneMeShallow();
            ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(cf.id());
            for (Column column : cf_)
            {
                cf.addColumn(column.localCopy(cfs), HeapAllocator.instance);
            }
            rm.add(cf);
        }
        rm.apply();
    }

    public void addAll(IMutation m)
    {
        if (!(m instanceof CounterMutation))
            throw new IllegalArgumentException();

        CounterMutation cm = (CounterMutation)m;
        rowMutation.addAll(cm.rowMutation);
    }

    @Override
    public String toString()
    {
        return toString(false);
    }

    public String toString(boolean shallow)
    {
        StringBuilder buff = new StringBuilder("CounterMutation(");
        buff.append(rowMutation.toString(shallow));
        buff.append(", ").append(consistency.toString());
        return buff.append(")").toString();
    }
}

class CounterMutationSerializer implements IVersionedSerializer<CounterMutation>
{
    public void serialize(CounterMutation cm, DataOutput out, int version) throws IOException
    {
        RowMutation.serializer.serialize(cm.rowMutation(), out, version);
        out.writeUTF(cm.consistency().name());
    }

    public CounterMutation deserialize(DataInput in, int version) throws IOException
    {
        RowMutation rm = RowMutation.serializer.deserialize(in, version);
        ConsistencyLevel consistency = Enum.valueOf(ConsistencyLevel.class, in.readUTF());
        return new CounterMutation(rm, consistency);
    }

    public long serializedSize(CounterMutation cm, int version)
    {
        return RowMutation.serializer.serializedSize(cm.rowMutation(), version)
             + TypeSizes.NATIVE.sizeof(cm.consistency().name());
    }
}
