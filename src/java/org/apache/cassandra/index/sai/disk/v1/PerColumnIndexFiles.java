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
import java.util.EnumMap;
import java.util.Map;

import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.utils.IndexIdentifier;
import org.apache.cassandra.index.sai.utils.IndexTermType;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.FileUtils;

/**
 * Maintains a mapping of {@link IndexComponent}s to associated {@link FileHandle}s for
 * read operations on the components. Users of this class are returned copies of the
 * {@link FileHandle}s using {@link FileHandle#sharedCopy()} so returned handles still
 * need to be closed by the user.
 */
public class PerColumnIndexFiles implements Closeable
{
    private final Map<IndexComponent, FileHandle> files = new EnumMap<>(IndexComponent.class);
    private final IndexDescriptor indexDescriptor;
    private final IndexIdentifier indexIdentifier;

    public PerColumnIndexFiles(IndexDescriptor indexDescriptor, IndexTermType indexTermType, IndexIdentifier indexIdentifier)
    {
        this.indexDescriptor = indexDescriptor;
        this.indexIdentifier = indexIdentifier;
        for (IndexComponent component : indexDescriptor.version.onDiskFormat().perColumnIndexComponents(indexTermType))
        {
            if (component == IndexComponent.META || component == IndexComponent.COLUMN_COMPLETION_MARKER)
                continue;
            files.put(component, indexDescriptor.createPerIndexFileHandle(component, indexIdentifier, this::close));
        }
    }

    public FileHandle termsData()
    {
        return getFile(IndexComponent.TERMS_DATA);
    }

    public FileHandle postingLists()
    {
        return getFile(IndexComponent.POSTING_LISTS);
    }

    public FileHandle balancedTree()
    {
        return getFile(IndexComponent.BALANCED_TREE);
    }

    public FileHandle compressedVectors()
    {
        return getFile(IndexComponent.COMPRESSED_VECTORS);
    }

    private FileHandle getFile(IndexComponent indexComponent)
    {
        FileHandle file = files.get(indexComponent);
        if (file == null)
            throw new IllegalArgumentException(String.format(indexIdentifier.logMessage("Component %s not found for SSTable %s"),
                                                             indexComponent, indexDescriptor.sstableDescriptor));

        return file.sharedCopy();
    }

    @Override
    public void close()
    {
        FileUtils.closeQuietly(files.values());
    }
}
