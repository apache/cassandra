/**
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

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.net.InetAddress;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.LinkedList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.db.marshal.AbstractCommutativeType;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.io.ICompactSerializer;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.thrift.ConsistencyLevel;

public class CounterMutation implements IMutation
{
    private static final Logger logger = LoggerFactory.getLogger(CounterMutation.class);
    private static final CounterMutationSerializer serializer = new CounterMutationSerializer();

    private final RowMutation rowMutation;
    private final ConsistencyLevel consistency;

    public CounterMutation(RowMutation rowMutation, ConsistencyLevel consistency)
    {
        this.rowMutation = rowMutation;
        this.consistency = consistency;
    }

    public String getTable()
    {
        return rowMutation.getTable();
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

    public static CounterMutationSerializer serializer()
    {
        return serializer;
    }

    public RowMutation makeReplicationMutation() throws IOException
    {
        List<ReadCommand> readCommands = new LinkedList<ReadCommand>();
        for (ColumnFamily columnFamily : rowMutation.getColumnFamilies())
        {
            if (!columnFamily.metadata().getReplicateOnWrite())
                continue;

            // CF type: regular
            if (!columnFamily.isSuper())
            {
                QueryPath queryPath = new QueryPath(columnFamily.metadata().cfName);
                ReadCommand readCommand = new SliceByNamesReadCommand(rowMutation.getTable(), rowMutation.key(), queryPath, columnFamily.getColumnNames());
                readCommands.add(readCommand);
                continue;
            }

            // CF type: super
            for (IColumn superColumn : columnFamily.getSortedColumns())
            {
                QueryPath queryPath = new QueryPath(columnFamily.metadata().cfName, superColumn.name());

                // construct set of sub-column names
                Collection<IColumn> subColumns = superColumn.getSubColumns();
                Collection<ByteBuffer> subColNames = new HashSet<ByteBuffer>(subColumns.size());
                for (IColumn subCol : subColumns)
                {
                    subColNames.add(subCol.name());
                }

                ReadCommand readCommand = new SliceByNamesReadCommand(rowMutation.getTable(), rowMutation.key(), queryPath, subColNames);
                readCommands.add(readCommand);
            }
        }

        // replicate to non-local replicas
        List<InetAddress> foreignReplicas = StorageService.instance.getLiveNaturalEndpoints(rowMutation.getTable(), rowMutation.key());
        foreignReplicas.remove(FBUtilities.getLocalAddress()); // remove local replica

        // create a replication RowMutation
        RowMutation replicationMutation = new RowMutation(rowMutation.getTable(), rowMutation.key());
        for (ReadCommand readCommand : readCommands)
        {
            Table table = Table.open(readCommand.table);
            Row row = readCommand.getRow(table);
            AbstractType defaultValidator = row.cf.metadata().getDefaultValidator();
            if (defaultValidator.isCommutative())
            {
                /**
                 * Clean out contexts for all nodes we're sending the repair to, otherwise,
                 * we could send a context which is local to one of the foreign replicas,
                 * which would then incorrectly add that to its own count, because
                 * local resolution aggregates.
                 */
                // note: the following logic could be optimized
                for (InetAddress foreignNode : foreignReplicas)
                {
                    ((AbstractCommutativeType)defaultValidator).cleanContext(row.cf, foreignNode);
                }
            }
            replicationMutation.add(row.cf);
        }
        return replicationMutation;
    }

    public Message makeMutationMessage(int version) throws IOException
    {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(bos);
        serializer().serialize(this, dos, version);
        return new Message(FBUtilities.getLocalAddress(), StorageService.Verb.COUNTER_MUTATION, bos.toByteArray(), version);
    }

    public boolean shouldReplicateOnWrite()
    {
        for (ColumnFamily cf : rowMutation.getColumnFamilies())
            if (cf.metadata().getReplicateOnWrite())
                return true;
        return false;
    }

    public void apply() throws IOException
    {
        // We need to transform all CounterUpdateColumn to CounterColumn and we need to deepCopy. Both are done 
        // below since CUC.asCounterColumn() does a deep copy.
        RowMutation rm = new RowMutation(rowMutation.getTable(), ByteBufferUtil.clone(rowMutation.key()));
        Table table = Table.open(rm.getTable());

        for (ColumnFamily cf_ : rowMutation.getColumnFamilies())
        {
            ColumnFamily cf = cf_.cloneMeShallow();
            ColumnFamilyStore cfs = table.getColumnFamilyStore(cf.id());
            for (IColumn column : cf_.getColumnsMap().values())
            {
                cf.addColumn(column.localCopy(null)); // TODO fix this
            }
            rm.add(cf);
        }
        rm.apply();
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

class CounterMutationSerializer implements ICompactSerializer<CounterMutation>
{
    public void serialize(CounterMutation cm, DataOutputStream dos, int version) throws IOException
    {
        RowMutation.serializer().serialize(cm.rowMutation(), dos, version);
        dos.writeUTF(cm.consistency().name());
    }

    public CounterMutation deserialize(DataInputStream dis, int version) throws IOException
    {
        RowMutation rm = RowMutation.serializer().deserialize(dis, version);
        ConsistencyLevel consistency = Enum.valueOf(ConsistencyLevel.class, dis.readUTF());
        return new CounterMutation(rm, consistency);
    }
}
