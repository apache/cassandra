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
import java.util.Objects;

public abstract class AbstractClusteringPrefix implements ClusteringPrefix
{
    public ClusteringPrefix clustering()
    {
        return this;
    }

    public int dataSize()
    {
        int size = 0;
        for (int i = 0; i < size(); i++)
        {
            ByteBuffer bb = get(i);
            size += bb == null ? 0 : bb.remaining();
        }
        return size;
    }

    public void digest(Digest digest)
    {
        for (int i = 0; i < size(); i++)
        {
            ByteBuffer bb = get(i);
            if (bb != null)
                digest.update(bb);
        }
        digest.updateWithByte(kind().ordinal());
    }

    @Override
    public final int hashCode()
    {
        int result = 31;
        for (int i = 0; i < size(); i++)
            result += 31 * Objects.hashCode(get(i));
        return 31 * result + Objects.hashCode(kind());
    }

    @Override
    public final boolean equals(Object o)
    {
        if(!(o instanceof ClusteringPrefix))
            return false;

        ClusteringPrefix that = (ClusteringPrefix)o;
        if (this.kind() != that.kind() || this.size() != that.size())
            return false;

        for (int i = 0; i < size(); i++)
            if (!Objects.equals(this.get(i), that.get(i)))
                return false;

        return true;
    }
}
