
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

public class ReusableClustering extends Clustering
{
    private static final long EMPTY_SIZE = ObjectSizes.measure(new ReusableClustering(0));

    protected final ByteBuffer[] values;

    protected ReusableWriter writer;

    public ReusableClustering(int size)
    {
        this.values = new ByteBuffer[size];
    }

    public int size()
    {
        return values.length;
    }

    public ByteBuffer get(int i)
    {
        return values[i];
    }

    public ByteBuffer[] getRawValues()
    {
        return values;
    }

    public Writer writer()
    {
        if (writer == null)
            writer = new ReusableWriter();
        return writer;
    }

    public void reset()
    {
        Arrays.fill(values, null);
        if (writer != null)
            writer.reset();
    }

    protected class ReusableWriter implements Writer
    {
        int idx;

        public void writeClusteringValue(ByteBuffer value)
        {
            values[idx++] = value;
        }

        private void reset()
        {
            idx = 0;
        }
    }
}
