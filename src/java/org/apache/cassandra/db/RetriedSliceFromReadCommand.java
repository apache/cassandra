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

import java.nio.ByteBuffer;

import org.apache.cassandra.db.filter.*;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RetriedSliceFromReadCommand extends SliceFromReadCommand
{
    static final Logger logger = LoggerFactory.getLogger(RetriedSliceFromReadCommand.class);
    public final int originalCount;

    public RetriedSliceFromReadCommand(String table, ByteBuffer key, ColumnParent column_parent, SliceQueryFilter filter, int originalCount)
    {
        this(table, key, new QueryPath(column_parent), filter, originalCount);
    }

    public RetriedSliceFromReadCommand(String table, ByteBuffer key, QueryPath path, SliceQueryFilter filter, int originalCount)
    {
        super(table, key, path, filter);
        this.originalCount = originalCount;
    }

    @Override
    public ReadCommand copy()
    {
        ReadCommand readCommand = new RetriedSliceFromReadCommand(table, key, queryPath, filter, originalCount);
        readCommand.setDigestQuery(isDigestQuery());
        return readCommand;
    }

    @Override
    public int getOriginalRequestedCount()
    {
        return originalCount;
    }

    @Override
    public String toString()
    {
        return "RetriedSliceFromReadCommand(" + "cmd=" + super.toString() + ", originalCount=" + originalCount + ")";
    }

}
