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

import org.apache.cassandra.db.marshal.ByteBufferAccessor;
import org.apache.cassandra.db.marshal.ValueAccessor;
import org.apache.cassandra.utils.ByteBufferUtil;

public abstract class AbstractBufferClusteringPrefix extends AbstractOnHeapClusteringPrefix<ByteBuffer>
{
    public static final ByteBuffer[] EMPTY_VALUES_ARRAY = new ByteBuffer[0];

    protected AbstractBufferClusteringPrefix(Kind kind, ByteBuffer[] values)
    {
        super(kind, values);
    }

    public ValueAccessor<ByteBuffer> accessor()
    {
        return ByteBufferAccessor.instance;
    }

    public ByteBuffer[] getBufferArray()
    {
        return getRawValues();
    }

    @Override
    public ClusteringPrefix<ByteBuffer> retainable()
    {
        if (!ByteBufferUtil.canMinimize(values))
            return this;

        ByteBuffer[] minimizedValues = ByteBufferUtil.minimizeBuffers(this.values);
        if (kind.isBoundary())
            return accessor().factory().boundary(kind, minimizedValues);
        if (kind.isBound())
            return accessor().factory().bound(kind, minimizedValues);

        assert kind() != Kind.STATIC_CLUSTERING;    // not minimizable
        return accessor().factory().clustering(minimizedValues);
    }
}
