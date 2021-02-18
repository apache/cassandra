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
package org.apache.cassandra.index.sai.disk.v1;

import java.io.IOException;

import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;

public class NumericValuesMeta
{
    final long valueCount;
    final int blockSize;
    final long blockMetaOffset;

    NumericValuesMeta(IndexInput input) throws IOException
    {
        valueCount = input.readLong();
        blockSize = input.readInt();
        blockMetaOffset = input.readVLong();
    }

    public NumericValuesMeta(long valueCount, int blockSize, long blockMetaOffset)
    {
        this.valueCount = valueCount;
        this.blockSize = blockSize;
        this.blockMetaOffset = blockMetaOffset;
    }

    public void write(IndexOutput out) throws IOException
    {
        out.writeLong(valueCount);
        out.writeInt(blockSize);
        out.writeVLong(blockMetaOffset);
    }
}
