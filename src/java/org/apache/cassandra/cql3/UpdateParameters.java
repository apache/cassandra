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
import java.util.List;

import org.apache.cassandra.db.*;

/**
 * A simple container that simplify passing parameters for collections methods.
 */
public class UpdateParameters
{
    public final List<ByteBuffer> variables;
    public final long timestamp;
    private final int ttl;
    public final int localDeletionTime;

    public UpdateParameters(List<ByteBuffer> variables, long timestamp, int ttl)
    {
        this.variables = variables;
        this.timestamp = timestamp;
        this.ttl = ttl;
        this.localDeletionTime = (int)(System.currentTimeMillis() / 1000);
    }

    public Column makeColumn(ByteBuffer name, ByteBuffer value)
    {
        return ttl > 0
             ? new ExpiringColumn(name, value, timestamp, ttl)
             : new Column(name, value, timestamp);
    }

    public Column makeTombstone(ByteBuffer name)
    {
        return new DeletedColumn(name, localDeletionTime, timestamp);
    }

    public RangeTombstone makeRangeTombstone(ByteBuffer start, ByteBuffer end)
    {
        return new RangeTombstone(start, end, timestamp, localDeletionTime);
    }

    public RangeTombstone makeTombstoneForOverwrite(ByteBuffer start, ByteBuffer end)
    {
        return new RangeTombstone(start, end, timestamp - 1, localDeletionTime);
    }
}
