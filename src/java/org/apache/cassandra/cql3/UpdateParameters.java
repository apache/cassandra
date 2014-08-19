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
package org.apache.cassandra.cql3;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.cql3.statements.ColumnGroupMap;
import org.apache.cassandra.db.*;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.utils.Pair;

/**
 * A simple container that simplify passing parameters for collections methods.
 */
public class UpdateParameters
{
    public final CFMetaData metadata;
    public final List<ByteBuffer> variables;
    public final long timestamp;
    private final int ttl;
    public final int localDeletionTime;

    // For lists operation that require a read-before-write. Will be null otherwise.
    private final Map<ByteBuffer, ColumnGroupMap> prefetchedLists;

    public UpdateParameters(CFMetaData metadata, List<ByteBuffer> variables, long timestamp, int ttl, Map<ByteBuffer, ColumnGroupMap> prefetchedLists)
    {
        this.metadata = metadata;
        this.variables = variables;
        this.timestamp = timestamp;
        this.ttl = ttl;
        this.localDeletionTime = (int)(System.currentTimeMillis() / 1000);
        this.prefetchedLists = prefetchedLists;
    }

    public Column makeColumn(ByteBuffer name, ByteBuffer value) throws InvalidRequestException
    {
        QueryProcessor.validateCellName(name);
        return Column.create(name, value, timestamp, ttl, metadata);
    }

    public Column makeCounter(ByteBuffer name, long delta) throws InvalidRequestException
    {
        QueryProcessor.validateCellName(name);
        return new CounterUpdateColumn(name, delta, System.currentTimeMillis());
    }

    public Column makeTombstone(ByteBuffer name) throws InvalidRequestException
    {
        QueryProcessor.validateCellName(name);
        return new DeletedColumn(name, localDeletionTime, timestamp);
    }

    public RangeTombstone makeRangeTombstone(ByteBuffer start, ByteBuffer end) throws InvalidRequestException
    {
        QueryProcessor.validateCellName(start);
        QueryProcessor.validateCellName(end);
        return new RangeTombstone(start, end, timestamp, localDeletionTime);
    }

    public RangeTombstone makeTombstoneForOverwrite(ByteBuffer start, ByteBuffer end) throws InvalidRequestException
    {
        QueryProcessor.validateCellName(start);
        QueryProcessor.validateCellName(end);
        return new RangeTombstone(start, end, timestamp - 1, localDeletionTime);
    }

    public List<Pair<ByteBuffer, Column>> getPrefetchedList(ByteBuffer rowKey, ByteBuffer cql3ColumnName)
    {
        if (prefetchedLists == null)
            return Collections.emptyList();

        ColumnGroupMap m = prefetchedLists.get(rowKey);
        return m == null ? Collections.<Pair<ByteBuffer, Column>>emptyList() : m.getCollection(cql3ColumnName);
    }
}
