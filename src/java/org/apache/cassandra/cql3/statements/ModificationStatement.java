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
package org.apache.cassandra.cql3.statements;

import java.nio.ByteBuffer;
import java.util.*;

import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.cql3.*;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.ColumnSlice;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.db.filter.SliceQueryFilter;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.exceptions.*;
import org.apache.cassandra.db.IMutation;
import org.apache.cassandra.db.ExpiringColumn;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.transport.messages.ResultMessage;

/**
 * Abstract class for statements that apply on a given column family.
 */
public abstract class ModificationStatement extends CFStatement implements CQLStatement
{
    public static final ConsistencyLevel defaultConsistency = ConsistencyLevel.ONE;

    public static enum Type
    {
        LOGGED, UNLOGGED, COUNTER
    }

    protected Type type;

    private Long timestamp;
    private final int timeToLive;

    public ModificationStatement(CFName name, Attributes attrs)
    {
        this(name, attrs.timestamp, attrs.timeToLive);
    }

    public ModificationStatement(CFName name, Long timestamp, int timeToLive)
    {
        super(name);
        this.timestamp = timestamp;
        this.timeToLive = timeToLive;
    }

    public void checkAccess(ClientState state) throws InvalidRequestException, UnauthorizedException
    {
        state.hasColumnFamilyAccess(keyspace(), columnFamily(), Permission.MODIFY);
    }

    public void validate(ClientState state) throws InvalidRequestException
    {
        if (timeToLive < 0)
            throw new InvalidRequestException("A TTL must be greater or equal to 0");

        if (timeToLive > ExpiringColumn.MAX_TTL)
            throw new InvalidRequestException(String.format("ttl is too large. requested (%d) maximum (%d)", timeToLive, ExpiringColumn.MAX_TTL));
    }

    protected abstract void validateConsistency(ConsistencyLevel cl) throws InvalidRequestException;

    public ResultMessage execute(ConsistencyLevel cl, QueryState queryState, List<ByteBuffer> variables) throws RequestExecutionException, RequestValidationException
    {
        if (cl == null)
            throw new InvalidRequestException("Invalid empty consistency level");

        validateConsistency(cl);

        Collection<? extends IMutation> mutations = getMutations(variables, false, cl, queryState.getTimestamp());

        // The type should have been set by now or we have a bug
        assert type != null;

        switch (type)
        {
            case LOGGED:
                if (mutations.size() > 1)
                    StorageProxy.mutateAtomically((Collection<RowMutation>) mutations, cl);
                else
                    StorageProxy.mutate(mutations, cl);
                break;
            case UNLOGGED:
            case COUNTER:
                StorageProxy.mutate(mutations, cl);
                break;
            default:
                throw new AssertionError();
        }

        return null;
    }

    public ResultMessage executeInternal(QueryState queryState) throws RequestValidationException, RequestExecutionException
    {
        for (IMutation mutation : getMutations(Collections.<ByteBuffer>emptyList(), true, null, queryState.getTimestamp()))
            mutation.apply();
        return null;
    }

    public long getTimestamp(long now)
    {
        return timestamp == null ? now : timestamp;
    }

    public void setTimestamp(long timestamp)
    {
        this.timestamp = timestamp;
    }

    public boolean isSetTimestamp()
    {
        return timestamp != null;
    }

    public int getTimeToLive()
    {
        return timeToLive;
    }

    protected Map<ByteBuffer, ColumnGroupMap> readRows(List<ByteBuffer> keys, ColumnNameBuilder builder, Set<ByteBuffer> toRead, CompositeType composite, boolean local, ConsistencyLevel cl)
    throws RequestExecutionException, RequestValidationException
    {
        try
        {
            cl.validateForRead(keyspace());
        }
        catch (InvalidRequestException e)
        {
            throw new InvalidRequestException(String.format("Write operation require a read but consistency %s is not supported on reads", cl));
        }

        ColumnSlice[] slices = new ColumnSlice[toRead.size()];
        int i = 0;
        for (ByteBuffer name : toRead)
        {
            ByteBuffer start = builder.copy().add(name).build();
            ByteBuffer finish = builder.copy().add(name).buildAsEndOfRange();
            slices[i++] = new ColumnSlice(start, finish);
        }

        List<ReadCommand> commands = new ArrayList<ReadCommand>(keys.size());
        for (ByteBuffer key : keys)
            commands.add(new SliceFromReadCommand(keyspace(),
                                                  key,
                                                  new QueryPath(columnFamily()),
                                                  new SliceQueryFilter(slices, false, Integer.MAX_VALUE)));

        List<Row> rows = local
                       ? SelectStatement.readLocally(keyspace(), commands)
                       : StorageProxy.read(commands, cl);

        Map<ByteBuffer, ColumnGroupMap> map = new HashMap<ByteBuffer, ColumnGroupMap>();
        for (Row row : rows)
        {
            if (row.cf == null || row.cf.isEmpty())
                continue;

            ColumnGroupMap.Builder groupBuilder = new ColumnGroupMap.Builder(composite, true);
            for (IColumn column : row.cf)
                groupBuilder.add(column);

            List<ColumnGroupMap> groups = groupBuilder.groups();
            assert groups.isEmpty() || groups.size() == 1;
            if (!groups.isEmpty())
                map.put(row.key.key, groups.get(0));
        }
        return map;
    }

    /**
     * Convert statement into a list of mutations to apply on the server
     *
     * @param variables value for prepared statement markers
     * @param local if true, any requests (for collections) performed by getMutation should be done locally only.
     * @param cl the consistency to use for the potential reads involved in generating the mutations (for lists set/delete operations)
     * @param now the current timestamp in microseconds to use if no timestamp is user provided.
     *
     * @return list of the mutations
     * @throws InvalidRequestException on invalid requests
     */
    protected abstract Collection<? extends IMutation> getMutations(List<ByteBuffer> variables, boolean local, ConsistencyLevel cl, long now)
    throws RequestExecutionException, RequestValidationException;

    public abstract ParsedStatement.Prepared prepare(ColumnSpecification[] boundNames) throws InvalidRequestException;
}
