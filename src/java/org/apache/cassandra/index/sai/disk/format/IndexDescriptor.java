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

package org.apache.cassandra.index.sai.disk.format;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;
import com.google.common.io.Files;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.lifecycle.LifecycleNewTracker;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.IndexValidation;
import org.apache.cassandra.index.sai.SSTableContext;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.disk.PerColumnIndexWriter;
import org.apache.cassandra.index.sai.disk.PerSSTableIndexWriter;
import org.apache.cassandra.index.sai.disk.PrimaryKeyMap;
import org.apache.cassandra.index.sai.disk.RowMapping;
import org.apache.cassandra.index.sai.disk.SSTableIndex;
import org.apache.cassandra.index.sai.disk.io.IndexFileUtils;
import org.apache.cassandra.index.sai.disk.io.IndexOutputWriter;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Throwables;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.IOUtils;

/**
 * The {@link IndexDescriptor} is an analog of the SSTable {@link Descriptor} and provides version
 * specific information about the on-disk state of a {@link StorageAttachedIndex}.
 * <p>
 * The {@link IndexDescriptor} is primarily responsible for maintaining a view of the on-disk state
 * of an index for a specific {@link org.apache.cassandra.io.sstable.SSTable}.
 * <p>
 * It is responsible for opening files for use by writers and readers.
 * <p>
 * Its remaining responsibility is to act as a proxy to the {@link OnDiskFormat} associated with the
 * index {@link Version}.
 */
public class IndexDescriptor
{
    private static final Logger logger = LoggerFactory.getLogger(IndexDescriptor.class);

    public final Version version;
    public final Descriptor sstableDescriptor;
    public final ClusteringComparator clusteringComparator;
    public final PrimaryKey.Factory primaryKeyFactory;

    private IndexDescriptor(Version version, Descriptor sstableDescriptor, ClusteringComparator clusteringComparator)
    {
        this.version = version;
        this.sstableDescriptor = sstableDescriptor;
        this.clusteringComparator = clusteringComparator;
        this.primaryKeyFactory = new PrimaryKey.Factory(clusteringComparator);
    }

    public static IndexDescriptor create(Descriptor descriptor, ClusteringComparator clusteringComparator)
    {
        return new IndexDescriptor(Version.LATEST, descriptor, clusteringComparator);
    }

    public static IndexDescriptor create(SSTableReader sstable)
    {
        for (Version version : Version.ALL)
        {
            IndexDescriptor indexDescriptor = new IndexDescriptor(version,
                                                                  sstable.descriptor,
                                                                  sstable.metadata().comparator);

            if (version.onDiskFormat().isPerSSTableIndexBuildComplete(indexDescriptor))
            {
                return indexDescriptor;
            }
        }
        return new IndexDescriptor(Version.LATEST,
                                   sstable.descriptor,
                                   sstable.metadata().comparator);
    }

    public boolean hasClustering()
    {
        return clusteringComparator.size() > 0;
    }

    public String componentName(IndexComponent indexComponent)
    {
        return version.fileNameFormatter().format(indexComponent, null);
    }

    public PrimaryKeyMap.Factory newPrimaryKeyMapFactory(SSTableReader sstable)
    {
        return version.onDiskFormat().newPrimaryKeyMapFactory(this, sstable);
    }

    public SSTableIndex newSSTableIndex(SSTableContext sstableContext, IndexContext indexContext)
    {
        return version.onDiskFormat().newSSTableIndex(sstableContext, indexContext);
    }

    public PerSSTableIndexWriter newPerSSTableIndexWriter() throws IOException
    {
        return version.onDiskFormat().newPerSSTableIndexWriter(this);
    }

    public PerColumnIndexWriter newPerColumnIndexWriter(StorageAttachedIndex index,
                                                        LifecycleNewTracker tracker,
                                                        RowMapping rowMapping)
    {
        return version.onDiskFormat().newPerColumnIndexWriter(index, this, tracker, rowMapping);
    }

    public boolean isPerSSTableIndexBuildComplete()
    {
        return version.onDiskFormat().isPerSSTableIndexBuildComplete(this);
    }

    public boolean isPerColumnIndexBuildComplete(IndexContext indexContext)
    {
        return version.onDiskFormat().isPerColumnIndexBuildComplete(this, indexContext);
    }

    public boolean hasComponent(IndexComponent indexComponent)
    {
        return fileFor(indexComponent).exists();
    }

    public boolean hasComponent(IndexComponent indexComponent, IndexContext indexContext)
    {
        return fileFor(indexComponent, indexContext).exists();
    }

    public File fileFor(IndexComponent indexComponent)
    {
        return createFile(indexComponent, null);
    }

    public File fileFor(IndexComponent indexComponent, IndexContext indexContext)
    {
        return createFile(indexComponent, indexContext);
    }

    public boolean isIndexEmpty(IndexContext indexContext)
    {
        // The index is empty if the index build completed successfully in that both
        // a GROUP_COMPLETION_MARKER companent and a COLUMN_COMPLETION_MARKER exist for
        // the index and the number of per-index components is 1 indicating that only the
        // COLUMN_COMPLETION_MARKER exists for the index, as this is the only file that
        // will be written if the index is empty
        return isPerColumnIndexBuildComplete(indexContext) && numberOfPerIndexComponents(indexContext) == 1;
    }

    public void createComponentOnDisk(IndexComponent component) throws IOException
    {
        Files.touch(fileFor(component).toJavaIOFile());
    }

    public void createComponentOnDisk(IndexComponent component, IndexContext indexContext) throws IOException
    {
        Files.touch(fileFor(component, indexContext).toJavaIOFile());
    }

    public IndexInput openPerSSTableInput(IndexComponent indexComponent)
    {
        File file = fileFor(indexComponent);
        if (logger.isTraceEnabled())
            logger.trace(logMessage("Opening blocking index input for file {} ({})"),
                         file,
                         FBUtilities.prettyPrintMemory(file.length()));

        return IndexFileUtils.instance.openBlockingInput(file);
    }

    public IndexInput openPerIndexInput(IndexComponent indexComponent, IndexContext indexContext)
    {
        final File file = fileFor(indexComponent, indexContext);
        if (logger.isTraceEnabled())
            logger.trace(logMessage("Opening blocking index input for file {} ({})"),
                         file,
                         FBUtilities.prettyPrintMemory(file.length()));

        return IndexFileUtils.instance.openBlockingInput(file);
    }

    public IndexOutputWriter openPerSSTableOutput(IndexComponent component) throws IOException
    {
        return openPerSSTableOutput(component, false);
    }

    public IndexOutputWriter openPerSSTableOutput(IndexComponent component, boolean append) throws IOException
    {
        final File file = fileFor(component);

        if (logger.isTraceEnabled())
            logger.trace(logMessage("Creating SSTable attached index output for component {} on file {}..."),
                         component,
                         file);

        IndexOutputWriter writer = IndexFileUtils.instance.openOutput(file);

        if (append)
        {
            writer.skipBytes(file.length());
        }

        return writer;
    }

    public IndexOutputWriter openPerIndexOutput(IndexComponent indexComponent, IndexContext indexContext) throws IOException
    {
        return openPerIndexOutput(indexComponent, indexContext, false);
    }

    public IndexOutputWriter openPerIndexOutput(IndexComponent component, IndexContext indexContext, boolean append) throws IOException
    {
        final File file = fileFor(component, indexContext);

        if (logger.isTraceEnabled())
            logger.trace(indexContext.logMessage("Creating sstable attached index output for component {} on file {}..."),
                         component,
                         file);

        IndexOutputWriter writer = IndexFileUtils.instance.openOutput(file);

        if (append)
        {
            writer.skipBytes(file.length());
        }

        return writer;
    }

    public FileHandle createPerSSTableFileHandle(IndexComponent indexComponent, Throwables.DiscreteAction<?> cleanup)
    {
        try
        {
            final File file = fileFor(indexComponent);

            if (logger.isTraceEnabled())
            {
                logger.trace(logMessage("Opening {} file handle for {} ({})"),
                             file, FBUtilities.prettyPrintMemory(file.length()));
            }

            return new FileHandle.Builder(file).mmapped(true).complete();
        }
        catch (Throwable t)
        {
            throw handleFileHandleCleanup(t, cleanup);
        }
    }

    public FileHandle createPerIndexFileHandle(IndexComponent indexComponent, IndexContext indexContext, Throwables.DiscreteAction<?> cleanup)
    {
        try
        {
            final File file = fileFor(indexComponent, indexContext);

            if (logger.isTraceEnabled())
            {
                logger.trace(indexContext.logMessage("Opening file handle for {} ({})"),
                             file, FBUtilities.prettyPrintMemory(file.length()));
            }

            return new FileHandle.Builder(file).mmapped(true).complete();
        }
        catch (Throwable t)
        {
            throw handleFileHandleCleanup(t, cleanup);
        }
    }

    private RuntimeException handleFileHandleCleanup(Throwable t, Throwables.DiscreteAction<?> cleanup)
    {
        if (cleanup != null)
        {
            try
            {
                cleanup.perform();
            }
            catch (Exception e)
            {
                return Throwables.unchecked(Throwables.merge(t, e));
            }
        }
        return Throwables.unchecked(t);
    }

    public Set<Component> getLivePerSSTableComponents()
    {
        return version.onDiskFormat()
                      .perSSTableIndexComponents(hasClustering())
                      .stream()
                      .filter(c -> fileFor(c).exists())
                      .map(version::makePerSSTableComponent)
                      .collect(Collectors.toSet());
    }

    public Set<Component> getLivePerIndexComponents(IndexContext indexContext)
    {
        return version.onDiskFormat()
                      .perColumnIndexComponents(indexContext)
                      .stream()
                      .filter(c -> fileFor(c, indexContext).exists())
                      .map(c -> version.makePerIndexComponent(c, indexContext))
                      .collect(Collectors.toSet());
    }

    public long sizeOnDiskOfPerSSTableComponents()
    {
        return version.onDiskFormat()
                      .perSSTableIndexComponents(hasClustering())
                      .stream()
                      .map(this::fileFor)
                      .filter(File::exists)
                      .mapToLong(File::length)
                      .sum();
    }

    public long sizeOnDiskOfPerIndexComponents(IndexContext indexContext)
    {
        return version.onDiskFormat()
                      .perColumnIndexComponents(indexContext)
                      .stream()
                      .map(c -> fileFor(c, indexContext))
                      .filter(File::exists)
                      .mapToLong(File::length)
                      .sum();
    }

    @VisibleForTesting
    public long sizeOnDiskOfPerIndexComponent(IndexComponent indexComponent, IndexContext indexContext)
    {
        File componentFile = fileFor(indexComponent, indexContext);
        return componentFile.exists() ? componentFile.length() : 0;
    }

    @SuppressWarnings("BooleanMethodIsAlwaysInverted")
    public boolean validatePerIndexComponents(IndexContext indexContext, IndexValidation validation)
    {
        if (validation == IndexValidation.NONE)
            return true;

        logger.info(indexContext.logMessage("Validating per-column index components using mode " + validation));
        boolean checksum = validation == IndexValidation.CHECKSUM;

        try
        {
            version.onDiskFormat().validatePerColumnIndexComponents(this, indexContext, checksum);
            return true;
        }
        catch (UncheckedIOException e)
        {
            return false;
        }
    }

    @SuppressWarnings("BooleanMethodIsAlwaysInverted")
    public boolean validatePerSSTableComponents(IndexValidation validation)
    {
        if (validation == IndexValidation.NONE)
            return true;

        logger.info(logMessage("Validating per-sstable index components using mode " + validation));
        boolean checksum = validation == IndexValidation.CHECKSUM;

        try
        {
            version.onDiskFormat().validatePerSSTableIndexComponents(this, checksum);
            return true;
        }
        catch (UncheckedIOException e)
        {
            return false;
        }
    }

    public void checksumPerIndexComponents(IndexContext indexContext)
    {
        version.onDiskFormat().validatePerColumnIndexComponents(this, indexContext, true);
    }

    @SuppressWarnings("BooleanMethodIsAlwaysInverted")
    public void checksumPerSSTableComponents()
    {
        version.onDiskFormat().validatePerSSTableIndexComponents(this, true);
    }

    public void deletePerSSTableIndexComponents()
    {
        version.onDiskFormat()
               .perSSTableIndexComponents(hasClustering())
               .stream()
               .map(this::fileFor)
               .filter(File::exists)
               .forEach(this::deleteComponent);
    }

    public void deleteColumnIndex(IndexContext indexContext)
    {
        version.onDiskFormat()
               .perColumnIndexComponents(indexContext)
               .stream()
               .map(c -> fileFor(c, indexContext))
               .filter(File::exists)
               .forEach(this::deleteComponent);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(sstableDescriptor, version);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        IndexDescriptor other = (IndexDescriptor)o;
        return Objects.equal(sstableDescriptor, other.sstableDescriptor) &&
               Objects.equal(version, other.version);
    }

    @Override
    public String toString()
    {
        return sstableDescriptor.toString() + "-SAI";
    }

    public String logMessage(String message)
    {
        // Index names are unique only within a keyspace.
        return String.format("[%s.%s.*] %s",
                             sstableDescriptor.ksname,
                             sstableDescriptor.cfname,
                             message);
    }

    private File createFile(IndexComponent component, IndexContext indexContext)
    {
        Component customComponent = version.makePerIndexComponent(component, indexContext);
        return sstableDescriptor.fileFor(customComponent);
    }

    private long numberOfPerIndexComponents(IndexContext indexContext)
    {
        return version.onDiskFormat()
                      .perColumnIndexComponents(indexContext)
                      .stream()
                      .map(c -> fileFor(c, indexContext))
                      .filter(File::exists)
                      .count();
    }

    private void deleteComponent(File file)
    {
        logger.debug(logMessage("Deleting storage-attached index component file {}"), file);
        try
        {
            IOUtils.deleteFilesIfExist(file.toPath());
        }
        catch (IOException e)
        {
            logger.warn(logMessage("Unable to delete storage-attached index component file {} due to {}."), file, e.getMessage(), e);
        }
    }
}
