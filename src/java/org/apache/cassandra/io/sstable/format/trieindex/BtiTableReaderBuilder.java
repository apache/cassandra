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

package org.apache.cassandra.io.sstable.format.trieindex;

import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SSTableReaderBuilder;
import org.apache.cassandra.io.util.FileHandle;

public class BtiTableReaderBuilder extends SSTableReaderBuilder<BtiTableReader, BtiTableReaderBuilder>
{
    private PartitionIndex partitionIndex;
    private FileHandle rowIndexFile;

    public BtiTableReaderBuilder(Descriptor descriptor)
    {
        super(descriptor);
    }

    public BtiTableReaderBuilder setRowIndexFile(FileHandle rowIndexFile)
    {
        this.rowIndexFile = rowIndexFile;
        return this;
    }

    public BtiTableReaderBuilder setPartitionIndex(PartitionIndex partitionIndex)
    {
        this.partitionIndex = partitionIndex;
        return this;
    }

    public PartitionIndex getPartitionIndex()
    {
        return partitionIndex;
    }

    public FileHandle getRowIndexFile()
    {
        return rowIndexFile;
    }


    @Override
    protected BtiTableReader buildInternal()
    {
        return new BtiTableReader(this);
    }
}
