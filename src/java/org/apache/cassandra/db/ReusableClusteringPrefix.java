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
import java.util.Arrays;

import org.apache.cassandra.utils.ObjectSizes;

// Note that we abuse a bit ReusableClustering to store Slice.Bound infos, but it's convenient so ...
public class ReusableClusteringPrefix extends ReusableClustering
{
    private Kind kind;
    private int size;

    public ReusableClusteringPrefix(int size)
    {
        super(size);
    }

    public ClusteringPrefix get()
    {
        // We use ReusableClusteringPrefix when writing sstables (in ColumnIndex) and we
        // don't write static clustering there.
        assert kind != Kind.STATIC_CLUSTERING;
        if (kind == Kind.CLUSTERING)
        {
            assert values.length == size;
            return this;
        }

        return Slice.Bound.create(kind, Arrays.copyOfRange(values, 0, size));
    }

    public void copy(ClusteringPrefix clustering)
    {
        kind = clustering.kind();
        for (int i = 0; i < clustering.size(); i++)
            values[i] = clustering.get(i);
        size = clustering.size();
    }
}
