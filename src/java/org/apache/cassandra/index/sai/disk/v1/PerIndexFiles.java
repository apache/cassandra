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

import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.utils.TypeUtil;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.FileUtils;

public class PerIndexFiles implements Closeable
{
    private final Map<IndexComponent, FileHandle> files = new EnumMap<>(IndexComponent.class);
    private final IndexDescriptor indexDescriptor;
    private final IndexContext indexContext;

    public PerIndexFiles(IndexDescriptor indexDescriptor, IndexContext indexContext, boolean temporary)
    {
        this.indexDescriptor = indexDescriptor;
        this.indexContext = indexContext;
        for (IndexComponent component : indexDescriptor.version.onDiskFormat().perIndexComponents(indexContext))
        {
            if (component == IndexComponent.META || component == IndexComponent.COLUMN_COMPLETION_MARKER)
                continue;
            files.put(component, indexDescriptor.createPerIndexFileHandle(component, indexContext, temporary));
        }
    }

    /** It is the caller's responsibility to close the returned file handle. */
    public FileHandle termsData()
    {
        return getFile(IndexComponent.TERMS_DATA).sharedCopy();
    }

    /** It is the caller's responsibility to close the returned file handle. */
    public FileHandle postingLists()
    {
        return getFile(IndexComponent.POSTING_LISTS).sharedCopy();
    }

    /** It is the caller's responsibility to close the returned file handle. */
    public FileHandle kdtree()
    {
        return getFile(IndexComponent.KD_TREE).sharedCopy();
    }

    /** It is the caller's responsibility to close the returned file handle. */
    public FileHandle kdtreePostingLists()
    {
        return getFile(IndexComponent.KD_TREE_POSTING_LISTS).sharedCopy();
    }

    /** It is the caller's responsibility to close the returned file handle. */
    public FileHandle vectors()
    {
        return getFile(IndexComponent.VECTOR).sharedCopy();
    }

    /** It is the caller's responsibility to close the returned file handle. */
    public FileHandle pq()
    {
        return getFile(IndexComponent.PQ).sharedCopy();
    }

    private FileHandle getFile(IndexComponent indexComponent)
    {
        FileHandle file = files.get(indexComponent);
        if (file == null)
            throw new IllegalArgumentException(String.format(indexContext.logMessage("Component %s not found for SSTable %s"),
                                                             indexComponent,
                                                             indexDescriptor.descriptor));

        return file;
    }

    @Override
    public void close()
    {
        FileUtils.closeQuietly(files.values());
    }
}
