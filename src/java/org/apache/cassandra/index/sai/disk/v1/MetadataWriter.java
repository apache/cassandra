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

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.concurrent.NotThreadSafe;

import org.apache.cassandra.index.sai.disk.ResettableByteBuffersIndexOutput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;

@NotThreadSafe
public class MetadataWriter implements Closeable
{
    private final IndexOutput output;
    private final Map<String, BytesRef> map = new HashMap<>();

    public MetadataWriter(IndexOutput output)
    {
        this.output = output;
    }

    public Builder builder(String name)
    {
        return new Builder(name);
    }

    public class Builder extends ResettableByteBuffersIndexOutput implements Closeable
    {
        private Builder(String name)
        {
            super(name);
        }

        @Override
        public void close()
        {
            map.put(getName(), new BytesRef(toArrayCopy(), 0, intSize()));
        }
    }

    private void finish() throws IOException
    {
        SAICodecUtils.writeHeader(output);
        output.writeInt(map.size());
        for (Map.Entry<String, BytesRef> entry : map.entrySet())
        {
            output.writeString(entry.getKey());
            output.writeInt(entry.getValue().length);
            output.writeBytes(entry.getValue().bytes, entry.getValue().offset, entry.getValue().length);
        }
        SAICodecUtils.writeFooter(output);
    }

    @Override
    public void close() throws IOException
    {
        try
        {
            finish();
        }
        finally
        {
            output.close();
        }
    }
}
