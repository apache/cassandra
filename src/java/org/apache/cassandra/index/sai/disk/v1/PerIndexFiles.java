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
import java.io.FileNotFoundException;
import java.io.UncheckedIOException;
import java.nio.file.NoSuchFileException;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.Map;

import org.slf4j.Logger;

import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.v2.V2OnDiskFormat;
import org.apache.cassandra.index.sai.disk.v3.V3OnDiskFormat;
import org.apache.cassandra.index.sai.utils.TypeUtil;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.FileUtils;

public class PerIndexFiles implements Closeable
{
    private static final Logger logger = org.slf4j.LoggerFactory.getLogger(PerIndexFiles.class);

    private final Map<IndexComponent, FileHandle> files = new EnumMap<>(IndexComponent.class);
    private final IndexDescriptor indexDescriptor;
    private final IndexContext indexContext;

    public PerIndexFiles(IndexDescriptor indexDescriptor, IndexContext indexContext)
    {
        this.indexDescriptor = indexDescriptor;
        this.indexContext = indexContext;

        // FIXME we only have one IndexDescriptor + Version per sstable, so this is a hack
        // to support indexes at different versions.  Vectors are the only types impacted by multiple versions so far.
        var toOpen = new HashSet<IndexComponent>();
        if (indexContext.isVector())
        {
            toOpen.addAll(V2OnDiskFormat.VECTOR_COMPONENTS_V2);
            toOpen.addAll(V3OnDiskFormat.VECTOR_COMPONENTS_V3);
        }
        else
        {
            toOpen.addAll(indexDescriptor.version.onDiskFormat().perIndexComponents(indexContext));
        }
        toOpen.remove(IndexComponent.META);
        toOpen.remove(IndexComponent.COLUMN_COMPLETION_MARKER);

        var componentsPresent = new HashSet<IndexComponent>();
        for (IndexComponent component : toOpen)
        {
            try
            {
                files.put(component, indexDescriptor.createPerIndexFileHandle(component, indexContext));
                componentsPresent.add(component);
            }
            catch (UncheckedIOException e)
            {
                // leave logging until we're done
            }
        }

        logger.info("Components present for {} are {}", indexDescriptor, componentsPresent);
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

    public FileHandle getFile(IndexComponent indexComponent)
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
