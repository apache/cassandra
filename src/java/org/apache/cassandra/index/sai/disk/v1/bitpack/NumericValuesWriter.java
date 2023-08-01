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
package org.apache.cassandra.index.sai.disk.v1.bitpack;

import java.io.Closeable;
import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.v1.MetadataWriter;
import org.apache.cassandra.index.sai.disk.v1.SAICodecUtils;
import org.apache.lucene.store.IndexOutput;

@NotThreadSafe
public class NumericValuesWriter implements Closeable
{
    public static final int MONOTONIC_BLOCK_SIZE = 16384;
    public static final int BLOCK_SIZE = 128;

    private final IndexOutput output;
    private final AbstractBlockPackedWriter writer;
    private final MetadataWriter metadataWriter;
    private final String componentName;
    private final int blockSize;
    private long count = 0;

    public NumericValuesWriter(String componentName,
                               IndexOutput indexOutput,
                               MetadataWriter metadataWriter,
                               boolean monotonic) throws IOException
    {
        this(componentName, indexOutput, metadataWriter, monotonic, monotonic ? MONOTONIC_BLOCK_SIZE : BLOCK_SIZE);
    }

    public NumericValuesWriter(IndexDescriptor indexDescriptor,
                               IndexComponent component,
                               MetadataWriter metadataWriter,
                               boolean monotonic,
                               int blockSize) throws IOException
    {
        this(indexDescriptor.componentName(component),
             indexDescriptor.openPerSSTableOutput(component),
             metadataWriter,
             monotonic,
             blockSize);
    }

    private NumericValuesWriter(String componentName,
                                IndexOutput indexOutput,
                                MetadataWriter metadataWriter,
                                boolean monotonic, int blockSize) throws IOException
    {
        SAICodecUtils.writeHeader(indexOutput);
        this.writer = monotonic ? new MonotonicBlockPackedWriter(indexOutput, blockSize)
                                : new BlockPackedWriter(indexOutput, blockSize);
        this.output = indexOutput;
        this.componentName = componentName;
        this.metadataWriter = metadataWriter;
        this.blockSize = blockSize;
    }

    @Override
    public void close() throws IOException
    {
        try (IndexOutput o = metadataWriter.builder(componentName))
        {
            long fp = writer.finish();
            SAICodecUtils.writeFooter(output);

            NumericValuesMeta.write(o, count, blockSize, fp);
        }
        finally
        {
            output.close();
        }
    }

    public void add(long value) throws IOException
    {
        writer.add(value);
        count++;
    }
}
