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

public abstract class AbstractClusteringPrefix<T> implements ClusteringPrefix<T>
{
    public ClusteringPrefix<T> clustering()
    {
        return this;
    }

    public int dataSize()
    {
        int size = 0;
        for (int i = 0; i < size(); i++)
        {
            T v = get(i);
            size += v == null ? 0 : accessor().size(v);
        }
        return size;
    }

    public void digest(Digest digest)
    {
        for (int i = 0; i < size(); i++)
        {
            T value = get(i);
            // FIXME: Modify Digest to take a value and accessor?
            ByteBuffer bb = accessor().toSafeBuffer(value);
            if (bb != null)
                digest.update(bb);
        }
        digest.updateWithByte(kind().ordinal());
    }

    @Override
    public final int hashCode()
    {
        return ClusteringPrefix.hashCode(this);
    }

    @Override
    public final boolean equals(Object o)
    {
        return ClusteringPrefix.equals(this, o);
    }
}
