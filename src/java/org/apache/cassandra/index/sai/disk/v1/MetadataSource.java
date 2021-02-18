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
import java.util.HashMap;
import java.util.Map;
import javax.annotation.concurrent.NotThreadSafe;

import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.index.sai.disk.io.IndexComponents;
import org.apache.cassandra.index.sai.utils.SAICodecUtils;
import org.apache.lucene.store.BufferedChecksumIndexInput;
import org.apache.lucene.store.ByteArrayIndexInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.BytesRef;

@NotThreadSafe
public class MetadataSource
{
    private final Version version;
    private final Map<String, BytesRef> components;

    private MetadataSource(Version version, Map<String, BytesRef> components)
    {
        this.version = version;
        this.components = components;
    }

    public static MetadataSource loadGroupMetadata(IndexComponents components) throws IOException
    {
        return MetadataSource.load(components.openBlockingInput(IndexComponents.GROUP_META));
    }

    public static MetadataSource loadColumnMetadata(IndexComponents components) throws IOException
    {
        return MetadataSource.load(components.openBlockingInput(components.meta));
    }

    private static MetadataSource load(IndexInput indexInput) throws IOException
    {
        Map<String, BytesRef> components = new HashMap<>();
        Version version;

        try (BufferedChecksumIndexInput input = new BufferedChecksumIndexInput(indexInput))
        {
            version = SAICodecUtils.checkHeader(input);
            final int num = input.readInt();

            for (int x = 0; x < num; x++)
            {
                if (input.length() == input.getFilePointer())
                {
                    // we should never get here, because we always add footer to the file
                    throw new IllegalStateException("Unexpected EOF in " + input);
                }

                final String name = input.readString();
                final int length = input.readInt();
                final byte[] bytes = new byte[length];
                input.readBytes(bytes, 0, length);

                components.put(name, new BytesRef(bytes));
            }

            SAICodecUtils.checkFooter(input);
        }

        return new MetadataSource(version, components);
    }

    public IndexInput get(String name)
    {
        BytesRef bytes = components.get(name);

        if (bytes == null)
        {
            throw new IllegalArgumentException(String.format("Could not find component '%s'. Available properties are %s.",
                                                             name, components.keySet()));
        }

        return new ByteArrayIndexInput(name, bytes.bytes);
    }

    public Version getVersion()
    {
        return version;
    }
}
