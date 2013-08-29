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
import java.util.*;

import org.apache.commons.lang3.StringUtils;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.ByteBufferUtil;

// TODO convert this to a Builder pattern instead of encouraging RM.add directly,
// which is less-efficient since we have to keep a mutable HashMap around
public class RowMutation implements IMutation
{
    public static final RowMutationSerializer serializer = new RowMutationSerializer();
    public static final String FORWARD_TO = "FWD_TO";
    public static final String FORWARD_FROM = "FWD_FRM";

    // todo this is redundant
    // when we remove it, also restore SerializationsTest.testRowMutationRead to not regenerate new RowMutations each test
    private final String keyspaceName;

    private final ByteBuffer key;
    // map of column family id to mutations for that column family.
    private final Map<UUID, ColumnFamily> modifications;

    public RowMutation(String keyspaceName, ByteBuffer key)
    {
        this(keyspaceName, key, new HashMap<UUID, ColumnFamily>());
    }

    public RowMutation(String keyspaceName, ByteBuffer key, ColumnFamily cf)
    {
        this(keyspaceName, key, Collections.singletonMap(cf.id(), cf));
    }

    public RowMutation(String keyspaceName, Row row)
    {
        this(keyspaceName, row.key.key, row.cf);
    }

    protected RowMutation(String keyspaceName, ByteBuffer key, Map<UUID, ColumnFamily> modifications)
    {
        this.keyspaceName = keyspaceName;
        this.key = key;
        this.modifications = modifications;
    }

    public RowMutation(ByteBuffer key, ColumnFamily cf)
    {
        this(cf.metadata().ksName, key, cf);
    }

    public String getKeyspaceName()
    {
        return keyspaceName;
    }

    public Collection<UUID> getColumnFamilyIds()
    {
        return modifications.keySet();
    }

    public ByteBuffer key()
    {
        return key;
    }

    public Collection<ColumnFamily> getColumnFamilies()
    {
        return modifications.values();
    }

    public ColumnFamily getColumnFamily(UUID cfId)
    {
        return modifications.get(cfId);
    }

    /*
     * Specify a column family name and the corresponding column
     * family object.
     * param @ cf - column family name
     * param @ columnFamily - the column family.
     */
    public void add(ColumnFamily columnFamily)
    {
        assert columnFamily != null;
        ColumnFamily prev = modifications.put(columnFamily.id(), columnFamily);
        if (prev != null)
            // developer error
            throw new IllegalArgumentException("ColumnFamily " + columnFamily + " already has modifications in this mutation: " + prev);
    }

    /**
     * @return the ColumnFamily in this RowMutation corresponding to @param cfName, creating an empty one if necessary.
     */
    public ColumnFamily addOrGet(String cfName)
    {
        return addOrGet(Schema.instance.getCFMetaData(keyspaceName, cfName));
    }

    public ColumnFamily addOrGet(CFMetaData cfm)
    {
        ColumnFamily cf = modifications.get(cfm.cfId);
        if (cf == null)
        {
            cf = TreeMapBackedSortedColumns.factory.create(cfm);
            modifications.put(cfm.cfId, cf);
        }
        return cf;
    }

    public boolean isEmpty()
    {
        return modifications.isEmpty();
    }

    public void add(String cfName, ByteBuffer name, ByteBuffer value, long timestamp, int timeToLive)
    {
        addOrGet(cfName).addColumn(name, value, timestamp, timeToLive);
    }

    public void addCounter(String cfName, ByteBuffer name, long value)
    {
        addOrGet(cfName).addCounter(name, value);
    }

    public void add(String cfName, ByteBuffer name, ByteBuffer value, long timestamp)
    {
        add(cfName, name, value, timestamp, 0);
    }

    public void delete(String cfName, long timestamp)
    {
        int localDeleteTime = (int) (System.currentTimeMillis() / 1000);
        addOrGet(cfName).delete(new DeletionInfo(timestamp, localDeleteTime));
    }

    public void delete(String cfName, ByteBuffer name, long timestamp)
    {
        int localDeleteTime = (int) (System.currentTimeMillis() / 1000);
        addOrGet(cfName).addTombstone(name, localDeleteTime, timestamp);
    }

    public void deleteRange(String cfName, ByteBuffer start, ByteBuffer end, long timestamp)
    {
        int localDeleteTime = (int) (System.currentTimeMillis() / 1000);
        addOrGet(cfName).addAtom(new RangeTombstone(start, end, timestamp, localDeleteTime));
    }

    public void addAll(IMutation m)
    {
        if (!(m instanceof RowMutation))
            throw new IllegalArgumentException();

        RowMutation rm = (RowMutation)m;
        if (!keyspaceName.equals(rm.keyspaceName) || !key.equals(rm.key))
            throw new IllegalArgumentException();

        for (Map.Entry<UUID, ColumnFamily> entry : rm.modifications.entrySet())
        {
            // It's slighty faster to assume the key wasn't present and fix if
            // not in the case where it wasn't there indeed.
            ColumnFamily cf = modifications.put(entry.getKey(), entry.getValue());
            if (cf != null)
                entry.getValue().resolve(cf);
        }
    }

    /*
     * This is equivalent to calling commit. Applies the changes to
     * to the keyspace that is obtained by calling Keyspace.open().
     */
    public void apply()
    {
        Keyspace ks = Keyspace.open(keyspaceName);
        ks.apply(this, ks.metadata.durableWrites);
    }

    public void applyUnsafe()
    {
        Keyspace.open(keyspaceName).apply(this, false);
    }

    public MessageOut<RowMutation> createMessage()
    {
        return createMessage(MessagingService.Verb.MUTATION);
    }

    public MessageOut<RowMutation> createMessage(MessagingService.Verb verb)
    {
        return new MessageOut<RowMutation>(verb, this, serializer);
    }

    public String toString()
    {
        return toString(false);
    }

    public String toString(boolean shallow)
    {
        StringBuilder buff = new StringBuilder("RowMutation(");
        buff.append("keyspace='").append(keyspaceName).append('\'');
        buff.append(", key='").append(ByteBufferUtil.bytesToHex(key)).append('\'');
        buff.append(", modifications=[");
        if (shallow)
        {
            List<String> cfnames = new ArrayList<String>(modifications.size());
            for (UUID cfid : modifications.keySet())
            {
                CFMetaData cfm = Schema.instance.getCFMetaData(cfid);
                cfnames.add(cfm == null ? "-dropped-" : cfm.cfName);
            }
            buff.append(StringUtils.join(cfnames, ", "));
        }
        else
            buff.append(StringUtils.join(modifications.values(), ", "));
        return buff.append("])").toString();
    }

    public RowMutation without(UUID cfId)
    {
        RowMutation rm = new RowMutation(keyspaceName, key);
        for (Map.Entry<UUID, ColumnFamily> entry : modifications.entrySet())
            if (!entry.getKey().equals(cfId))
                rm.add(entry.getValue());
        return rm;
    }

    public static class RowMutationSerializer implements IVersionedSerializer<RowMutation>
    {
        public void serialize(RowMutation rm, DataOutput out, int version) throws IOException
        {
            if (version < MessagingService.VERSION_20)
                out.writeUTF(rm.getKeyspaceName());

            ByteBufferUtil.writeWithShortLength(rm.key(), out);

            /* serialize the modifications in the mutation */
            int size = rm.modifications.size();
            out.writeInt(size);
            assert size > 0;
            for (Map.Entry<UUID, ColumnFamily> entry : rm.modifications.entrySet())
                ColumnFamily.serializer.serialize(entry.getValue(), out, version);
        }

        public RowMutation deserialize(DataInput in, int version, ColumnSerializer.Flag flag) throws IOException
        {
            String keyspaceName = null; // will always be set from cf.metadata but javac isn't smart enough to see that
            if (version < MessagingService.VERSION_20)
                keyspaceName = in.readUTF();

            ByteBuffer key = ByteBufferUtil.readWithShortLength(in);
            int size = in.readInt();
            assert size > 0;

            Map<UUID, ColumnFamily> modifications;
            if (size == 1)
            {
                ColumnFamily cf = deserializeOneCf(in, version, flag);
                modifications = Collections.singletonMap(cf.id(), cf);
                keyspaceName = cf.metadata().ksName;
            }
            else
            {
                modifications = new HashMap<UUID, ColumnFamily>();
                for (int i = 0; i < size; ++i)
                {
                    ColumnFamily cf = deserializeOneCf(in, version, flag);
                    modifications.put(cf.id(), cf);
                    keyspaceName = cf.metadata().ksName;
                }
            }

            return new RowMutation(keyspaceName, key, modifications);
        }

        private ColumnFamily deserializeOneCf(DataInput in, int version, ColumnSerializer.Flag flag) throws IOException
        {
            ColumnFamily cf = ColumnFamily.serializer.deserialize(in, UnsortedColumns.factory, flag, version);
            // We don't allow RowMutation with null column family, so we should never get null back.
            assert cf != null;
            return cf;
        }

        public RowMutation deserialize(DataInput in, int version) throws IOException
        {
            return deserialize(in, version, ColumnSerializer.Flag.FROM_REMOTE);
        }

        public long serializedSize(RowMutation rm, int version)
        {
            TypeSizes sizes = TypeSizes.NATIVE;
            int size = 0;

            if (version < MessagingService.VERSION_20)
                size += sizes.sizeof(rm.getKeyspaceName());

            int keySize = rm.key().remaining();
            size += sizes.sizeof((short) keySize) + keySize;

            size += sizes.sizeof(rm.modifications.size());
            for (Map.Entry<UUID,ColumnFamily> entry : rm.modifications.entrySet())
                size += ColumnFamily.serializer.serializedSize(entry.getValue(), TypeSizes.NATIVE, version);

            return size;
        }
    }
}
