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

import org.apache.cassandra.db.composites.CellName;
import org.apache.cassandra.db.filter.NamesQueryFilter;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.HeapAllocator;

public class CounterMutation implements IMutation
{
    public static final CounterMutationSerializer serializer = new CounterMutationSerializer();

    private final Mutation mutation;
    private final ConsistencyLevel consistency;

    public CounterMutation(Mutation mutation, ConsistencyLevel consistency)
    {
        this.mutation = mutation;
        this.consistency = consistency;
    }

    public String getKeyspaceName()
    {
        return mutation.getKeyspaceName();
    }

    public Collection<UUID> getColumnFamilyIds()
    {
        return mutation.getColumnFamilyIds();
    }

    public Collection<ColumnFamily> getColumnFamilies()
    {
        return mutation.getColumnFamilies();
    }

    public ByteBuffer key()
    {
        return mutation.key();
    }

    public ConsistencyLevel consistency()
    {
        return consistency;
    }

    public Mutation makeReplicationMutation()
    {
        List<ReadCommand> readCommands = new LinkedList<ReadCommand>();
        long timestamp = System.currentTimeMillis();
        for (ColumnFamily columnFamily : mutation.getColumnFamilies())
        {
            if (!columnFamily.metadata().getReplicateOnWrite())
                continue;
            addReadCommandFromColumnFamily(mutation.getKeyspaceName(), mutation.key(), columnFamily, timestamp, readCommands);
        }

        // create a replication Mutation
        Mutation replicationMutation = new Mutation(mutation.getKeyspaceName(), mutation.key());
        for (ReadCommand readCommand : readCommands)
        {
            Keyspace keyspace = Keyspace.open(readCommand.ksName);
            Row row = readCommand.getRow(keyspace);
            if (row == null || row.cf == null)
                continue;

            ColumnFamily cf = row.cf;
            replicationMutation.add(cf);
        }
        return replicationMutation;
    }

    private void addReadCommandFromColumnFamily(String keyspaceName, ByteBuffer key, ColumnFamily columnFamily, long timestamp, List<ReadCommand> commands)
    {
        SortedSet<CellName> s = new TreeSet<>(columnFamily.metadata().comparator);
        Iterables.addAll(s, columnFamily.getColumnNames());
        commands.add(new SliceByNamesReadCommand(keyspaceName, key, columnFamily.metadata().cfName, timestamp, new NamesQueryFilter(s)));
    }

    public MessageOut<CounterMutation> makeMutationMessage()
    {
        return new MessageOut<CounterMutation>(MessagingService.Verb.COUNTER_MUTATION, this, serializer);
    }

    public boolean shouldReplicateOnWrite()
    {
        for (ColumnFamily cf : mutation.getColumnFamilies())
            if (cf.metadata().getReplicateOnWrite())
                return true;
        return false;
    }

    public void apply()
    {
        // transform all CounterUpdateCell to CounterCell: accomplished by localCopy
        Mutation m = new Mutation(mutation.getKeyspaceName(), ByteBufferUtil.clone(mutation.key()));
        Keyspace keyspace = Keyspace.open(m.getKeyspaceName());

        for (ColumnFamily cf_ : mutation.getColumnFamilies())
        {
            ColumnFamily cf = cf_.cloneMeShallow();
            ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(cf.id());
            for (Cell cell : cf_)
            {
                cf.addColumn(cell.localCopy(cfs), HeapAllocator.instance);
            }
            m.add(cf);
        }
        m.apply();
    }

    public void addAll(IMutation m)
    {
        if (!(m instanceof CounterMutation))
            throw new IllegalArgumentException();

        CounterMutation cm = (CounterMutation)m;
        mutation.addAll(cm.mutation);
    }

    @Override
    public String toString()
    {
        return toString(false);
    }

    public String toString(boolean shallow)
    {
        StringBuilder buff = new StringBuilder("CounterMutation(");
        buff.append(mutation.toString(shallow));
        buff.append(", ").append(consistency.toString());
        return buff.append(")").toString();
    }

    public static class CounterMutationSerializer implements IVersionedSerializer<CounterMutation>
    {
        public void serialize(CounterMutation cm, DataOutput out, int version) throws IOException
        {
            Mutation.serializer.serialize(cm.mutation, out, version);
            out.writeUTF(cm.consistency.name());
        }

        public CounterMutation deserialize(DataInput in, int version) throws IOException
        {
            Mutation m = Mutation.serializer.deserialize(in, version);
            ConsistencyLevel consistency = Enum.valueOf(ConsistencyLevel.class, in.readUTF());
            return new CounterMutation(m, consistency);
        }

        public long serializedSize(CounterMutation cm, int version)
        {
            return Mutation.serializer.serializedSize(cm.mutation, version)
                    + TypeSizes.NATIVE.sizeof(cm.consistency.name());
        }
    }
}
