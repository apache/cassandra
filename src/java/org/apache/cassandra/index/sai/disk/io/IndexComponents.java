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
package org.apache.cassandra.index.sai.disk.io;

import java.io.File;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;
import java.util.zip.CRC32;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import com.google.common.base.Strings;
import com.google.common.collect.ObjectArrays;
import com.google.common.io.Files;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.index.sai.disk.SegmentMetadata;
import org.apache.cassandra.index.sai.disk.v1.MetadataSource;
import org.apache.cassandra.index.sai.disk.v1.NumericValuesMeta;
import org.apache.cassandra.index.sai.disk.v1.PostingsWriter;
import org.apache.cassandra.index.sai.disk.v1.TrieTermsDictionaryWriter;
import org.apache.cassandra.index.sai.utils.SAICodecUtils;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.io.util.SequentialWriter;
import org.apache.cassandra.io.util.SequentialWriterOption;
import org.apache.cassandra.schema.CompressionParams;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.bkd.BKDWriter;

/**
 * //TODO Need to consider how we handle encryption in OS
 * The {@link Component}s that storage-attached indexing attaches to an SSTable.
 *
 * It allows us to unify index file creation, and ensures they will follow the same naming convention.
 */
public class IndexComponents
{
    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    public static final String TYPE_PREFIX = "SAI";
    private static final String PER_SSTABLE_FILE_NAME_FORMAT = TYPE_PREFIX + "_%s.db";
//    public static final String LEGACY_PER_COLUMN_FILE_NAME_FORMAT = "%s_" + PER_SSTABLE_FILE_NAME_FORMAT;
    public static final String PER_COLUMN_FILE_NAME_FORMAT = TYPE_PREFIX + "_%s_%s.db";

    public static class IndexComponent extends Component
    {
        public final NDIType ndiType;

        private IndexComponent(NDIType ndiType, String name)
        {
            super(Type.CUSTOM, name);
            this.ndiType = ndiType;
        }
    }

    public enum NDIType
    {
        // per-column components
        /**
         * Stores multiple {@link SegmentMetadata}s.
         */
        META("Meta", false),
        /**
         * KDTree written by {@link BKDWriter} indexes mappings of term to one ore more segment row IDs
         * (segment row ID = SSTable row ID - segment row ID offset).
         */
        KD_TREE("KDTree", false),
        KD_TREE_POSTING_LISTS("KDTreePostingLists", false),
        /**
         * Term dictionary written by {@link TrieTermsDictionaryWriter} stores mappings of term and
         * file pointer to posting block on posting file.
         */
        TERMS_DATA("TermsData", false, false, true),
        /**
         * Stores postings written by {@link PostingsWriter}
         */
        POSTING_LISTS("PostingLists", false),
        /**
         * If present indicates that the column index build completed successfully
         */
        COLUMN_COMPLETION_MARKER("ColumnComplete", false, true),

        // per-sstable components
        /**
         * Partition key token value for rows including row tombstone and static row. (access key is rowId)
         */
        TOKEN_VALUES("TokenValues"),
        /**
         * Partition key offset in sstable data file for rows including row tombstone and static row. (access key is
         * rowId)
         */
        OFFSETS_VALUES("OffsetsValues"),
        /**
         * Stores {@link NumericValuesMeta} for {@link NDIType#TOKEN_VALUES} and {@link NDIType#OFFSETS_VALUES}.
         */
        GROUP_META("GroupMeta"),
        /**
         * If present indicates that the per-sstable index build completed successfully
         */
        GROUP_COMPLETION_MARKER("GroupComplete", true, true);

        public final String name;
        private final boolean perSSTable;
        private final boolean marker;
        private final boolean encryptable;

        NDIType(String name)
        {
            this(name, true, false);
        }

        NDIType(String name, boolean perSSTable)
        {
            this(name, perSSTable, false);
        }

        NDIType(String name, boolean perSSTable, boolean marker)
        {
            this.name = name;
            this.perSSTable = perSSTable;
            this.marker = marker;
            this.encryptable = false;
        }

        NDIType(String name, boolean perSSTable, boolean marker, boolean encryptable)
        {
            this.name = name;
            this.perSSTable = perSSTable;
            this.marker = marker;
            this.encryptable = encryptable;
        }

        public boolean encryptable()
        {
            return encryptable;
        }

        public boolean perSSTable()
        {
            return perSSTable;
        }

        public boolean completionMarker()
        {
            return marker;
        }

        private boolean perSegment()
        {
            return !perSSTable && this != META;
        }

        public IndexComponent newComponent()
        {
            assert perSSTable;
            String componentName = String.format(PER_SSTABLE_FILE_NAME_FORMAT, name);

            return new IndexComponent(this, componentName);
        }

        public IndexComponent newComponent(String indexName)
        {
            assert !perSSTable;
            String componentName = String.format(PER_COLUMN_FILE_NAME_FORMAT, indexName, name);

            return new IndexComponent(this, componentName);
        }

        @Override
        public String toString()
        {
            return name;
        }
    }

    public static final NDIType[] STRING_COMPONENTS = new NDIType[]{ NDIType.TERMS_DATA, NDIType.POSTING_LISTS };

    private static final NDIType[] NUMERIC_COMPONENTS = new NDIType[]{ NDIType.KD_TREE, NDIType.KD_TREE_POSTING_LISTS };

    private static final NDIType[] PER_COLUMN_COMPONENTS = new NDIType[]{ NDIType.COLUMN_COMPLETION_MARKER, NDIType.META };

    private static final NDIType[] NUMERIC_PER_COLUMN_COMPONENTS = ObjectArrays.concat(PER_COLUMN_COMPONENTS, NUMERIC_COMPONENTS, NDIType.class);

    private static final NDIType[] LITERAL_PER_COLUMN_COMPONENTS = ObjectArrays.concat(PER_COLUMN_COMPONENTS, STRING_COMPONENTS, NDIType.class);

    private static final NDIType[] ALL_PER_COLUMN_COMPONENTS = ObjectArrays.concat(NUMERIC_PER_COLUMN_COMPONENTS, STRING_COMPONENTS, NDIType.class);

    public static final IndexComponent TOKEN_VALUES = NDIType.TOKEN_VALUES.newComponent();

    public static final IndexComponent OFFSETS_VALUES = NDIType.OFFSETS_VALUES.newComponent();

    public static final IndexComponent GROUP_META = NDIType.GROUP_META.newComponent();

    public static final IndexComponent GROUP_COMPLETION_MARKER = NDIType.GROUP_COMPLETION_MARKER.newComponent();


    /**
     * Files that are shared by all storage-attached indexes for each SSTable
     */
    public static final List<IndexComponent> PER_SSTABLE_COMPONENTS = Arrays.asList(GROUP_COMPLETION_MARKER, TOKEN_VALUES, OFFSETS_VALUES, GROUP_META);

    public final IndexComponent termsData, postingLists, meta, groupCompletionMarker, kdTree, kdTreePostingLists, columnCompletionMarker;

    private static final SequentialWriterOption defaultWriterOption = SequentialWriterOption.newBuilder()
                                                                                            .trickleFsync(DatabaseDescriptor.getTrickleFsync())
                                                                                            .trickleFsyncByteInterval(DatabaseDescriptor.getTrickleFsyncIntervalInKb() * 1024)
                                                                                            .bufferType(BufferType.OFF_HEAP)
                                                                                            .finishOnClose(true)
                                                                                            .build();

    public final Descriptor descriptor;
    public final String indexName;

    private final SequentialWriterOption writerOption;
    private final CompressionParams compressionParams;

    IndexComponents(Descriptor descriptor, SequentialWriterOption sequentialWriterOption, CompressionParams compressionParams)
    {
        this(null, descriptor, sequentialWriterOption, compressionParams);
    }

    @VisibleForTesting
    IndexComponents(String indexName, Descriptor descriptor, SequentialWriterOption sequentialWriterOption, CompressionParams compressionParams)
    {
        this.indexName = indexName;
        this.descriptor = descriptor;
        this.writerOption = sequentialWriterOption;

        this.compressionParams = compressionParams;

        termsData = NDIType.TERMS_DATA.newComponent(indexName);
        postingLists = NDIType.POSTING_LISTS.newComponent(indexName);
        meta = NDIType.META.newComponent(indexName);
        groupCompletionMarker = NDIType.GROUP_COMPLETION_MARKER.newComponent();
        kdTree = NDIType.KD_TREE.newComponent(indexName);
        kdTreePostingLists = NDIType.KD_TREE_POSTING_LISTS.newComponent(indexName);
        columnCompletionMarker = NDIType.COLUMN_COMPLETION_MARKER.newComponent(indexName);
    }

    /**
     * Used to access per-sstable and per-index components
     */
    public static IndexComponents create(String indexName, SSTableReader ssTableReader)
    {
        return create(indexName, ssTableReader.descriptor, CryptoUtils.getCompressionParams(ssTableReader));
    }

    public static IndexComponents create(String indexName, Descriptor descriptor, CompressionParams params)
    {
        return new IndexComponents(indexName, descriptor, defaultWriterOption, params);
    }

    /**
     * Returns the sstable {@link Component}s for the specified column index, excluding the shared ones.
     */
    public static Set<IndexComponent> perColumnComponents(String indexName, boolean isLiteral)
    {
        return components(indexName, isLiteral ? LITERAL_PER_COLUMN_COMPONENTS : NUMERIC_PER_COLUMN_COMPONENTS);
    }

    /**
     * Used to access per-sstable shared components
     */
    public static IndexComponents perSSTable(Descriptor descriptor, CompressionParams params)
    {
        return new IndexComponents(descriptor, defaultWriterOption, params);
    }

    public static IndexComponents perSSTable(SSTableReader ssTableReader)
    {
        return perSSTable(ssTableReader.descriptor, CryptoUtils.getCompressionParams(ssTableReader));
    }

    /**
     * @return <code>true</code> if an index build successfully completed building the per-SSTable
     * components for the given SSTable
     */
    public static boolean isGroupIndexComplete(Descriptor descriptor)
    {
        return descriptor.fileFor(GROUP_COMPLETION_MARKER).exists();
    }

    /**
     * @return <code>true</code> if an index build successfully completed for the given column index
     */
    public static boolean isColumnIndexComplete(Descriptor descriptor, String indexName)
    {
        return isGroupIndexComplete(descriptor) && descriptor.fileFor(NDIType.COLUMN_COMPLETION_MARKER.newComponent(indexName)).exists();
    }

    /**
     * @return <code>true</code> if an index build successfully completed for the given column index but
     * the SSTable did not have any indexable rows relating to the index
     */
    public static boolean isColumnIndexEmpty(Descriptor descriptor, String indexName)
    {
        long numIndexFiles = components(indexName, ALL_PER_COLUMN_COMPONENTS).stream().map(descriptor::fileFor).filter(File::exists).count();
        return isColumnIndexComplete(descriptor, indexName) && (numIndexFiles == 1);
    }

    /**
     * Delete the per-SSTable index files from the filesystem
     */
    public static void deletePerSSTableIndexComponents(Descriptor descriptor)
    {
        PER_SSTABLE_COMPONENTS.stream()
                              .map(descriptor::fileFor)
                              .filter(File::exists)
                              .forEach(IndexComponents::deleteComponent);
    }

    private static Set<IndexComponent> components(String indexName, NDIType... types)
    {
        Set<IndexComponent> components = new HashSet<>(types.length);
        for (NDIType type : types)
        {
            components.add(type.newComponent(indexName));
        }
        return components;
    }

    private static void deleteComponent(File file)
    {
        logger.debug("Deleting storage attached index component file {}", file);
        try
        {
            IOUtils.deleteFilesIfExist(file.toPath());
        }
        catch (IOException e)
        {
            logger.warn("Unable to delete storage attached index component file {} due to {}.", file, e.getMessage(), e);
        }
    }

    /**
     * @return total size (in bytes) of column index components
     */
    public long sizeOfPerColumnComponents()
    {
        return sizeOf(components(indexName, ALL_PER_COLUMN_COMPONENTS));
    }

    /**
     * @return total size (in bytes) of per-SSTable index components
     */
    public long sizeOfPerSSTableComponents()
    {
        return sizeOf(PER_SSTABLE_COMPONENTS);
    }

    public long sizeOf(Collection<IndexComponent> components)
    {
        return components.stream().map(descriptor::fileFor).filter(File::exists).mapToLong(File::length).sum();
    }

    /**
     * A helper method for constructing consistent log messages for specific column indexes.
     *
     * Example: For the index "idx" in keyspace "ks" on table "tb", calling this method with the raw message
     * "Flushing new index segment..." will produce...
     *
     * "[ks.idx.tb] Flushing new index segment..."
     *
     * @param message The raw content of a logging message, without information identifying it with an index.
     *
     * @return A log message with the proper keyspace, table and index name prepended to it.
     */
    public String logMessage(String message)
    {
        // Index names are unique only within a keyspace.
        return String.format("[%s.%s.%s] %s", descriptor.ksname, descriptor.cfname, Strings.isNullOrEmpty(indexName) ? "*" : indexName, message);
    }

    /**
     * Delete the underlying per-column index files from the filesystem.
     */
    public void deleteColumnIndex()
    {
        Stream.of(ALL_PER_COLUMN_COMPONENTS)
              .map(type -> type.newComponent(indexName))
              .map(descriptor::fileFor)
              .filter(File::exists)
              .forEach(IndexComponents::deleteComponent);
    }

    public FileHandle createFileHandle(IndexComponent component)
    {
        return createFileHandle(component, false);
    }

    public FileHandle createFileHandle(IndexComponent component, boolean temporary)
    {
        final File file = temporary ? descriptor.tmpFileFor(component) : descriptor.fileFor(component);

        if (logger.isTraceEnabled())
        {
            logger.trace(logMessage("Opening {} file handle for {} ({})"), temporary ? "temporary" : "", file, FBUtilities.prettyPrintMemory(file.length()));
        }

        try (final FileHandle.Builder builder = new FileHandle.Builder(file.getAbsolutePath()).mmapped(true))
        {
            return builder.complete();
        }
    }

    public boolean validatePerSSTableComponentsChecksum()
    {
        for (IndexComponent component : PER_SSTABLE_COMPONENTS)
        {
            try
            {
                validateComponent(component, true);
            }
            catch (Throwable e)
            {
                return false;
            }
        }
        return true;
    }

    public boolean validatePerColumnComponentsChecksum(boolean isLiteral)
    {
        try
        {
            validatePerColumnComponents(isLiteral, true);
            return true;
        }
        catch (Throwable e)
        {
            logger.warn(logMessage("Checksum validation failed on SSTable {}."), descriptor, e);
            return false;
        }
    }

    public void validatePerSSTableComponents() throws IOException
    {
        for (IndexComponent component : PER_SSTABLE_COMPONENTS)
        {
            validateComponent(component, false);
        }
    }

    public void validatePerColumnComponents(boolean isLiteral) throws IOException
    {
        validatePerColumnComponents(isLiteral, false);
    }

    public IndexInput openInput(FileHandle handle)
    {
        return IndexInputReader.create(handle);
    }

    @SuppressWarnings("resource")
    public IndexInput openBlockingInput(IndexComponent component)
    {
        final File file = descriptor.fileFor(component);
        if (logger.isTraceEnabled())
            logger.trace(logMessage("Opening blocking index input for file {} ({})"), file, FBUtilities.prettyPrintMemory(file.length()));

        try (final FileHandle.Builder builder = new FileHandle.Builder(file.getAbsolutePath()))
        {
            final FileHandle fileHandle = builder.complete();
            final RandomAccessReader randomReader = fileHandle.createReader();

            return IndexInputReader.create(randomReader, fileHandle::close);
        }
    }

    public IndexOutputWriter createOutput(IndexComponent component) throws IOException
    {
        return createOutput(component, false);
    }

    public IndexOutputWriter createOutput(IndexComponent component, boolean append) throws IOException
    {
        return createOutput(component, append, false);
    }

    public IndexOutputWriter createOutput(IndexComponent component, boolean append, boolean temporary) throws IOException
    {
        final File file = temporary ? descriptor.tmpFileFor(component) : descriptor.fileFor(component);

        if (logger.isTraceEnabled())
            logger.trace(logMessage("Creating {} sstable attached index output for component {} on file {}..."), temporary ? "temporary" : "", component, file);

        IndexOutputWriter writer = createOutput(file, component.ndiType.encryptable());

        if (append)
        {
            writer.skipBytes(file.length());
        }

        return writer;
    }

    public void deleteTemporaryComponents()
    {
        Stream.of(ALL_PER_COLUMN_COMPONENTS)
              .map(type -> type.newComponent(indexName))
              .map(descriptor::tmpFileFor)
              .filter(File::exists)
              .forEach(IndexComponents::deleteComponent);
    }

    public void deleteTemporaryComponent(IndexComponent component) throws IOException
    {
        final File file = descriptor.tmpFileFor(component);

        if (file.exists())
            if (!file.delete())
                logger.warn("Failed to delete temporary file " + file);
    }

    public CompressionParams getCompressionParams()
    {
        return compressionParams;
    }

    @SuppressWarnings("resource")
    public IndexOutputWriter createOutput(File file, boolean encryptable)
    {
        assert writerOption.finishOnClose() : "IndexOutputWriter relies on close() to sync with disk.";

        return new IndexOutputWriter(new IncrementalChecksumSequentialWriter(file));
    }

    @VisibleForTesting
    public IndexOutputWriter createOutput(File file)
    {
        return createOutput(file, false);
    }

    public void createGroupCompletionMarker() throws IOException
    {
        Files.touch(descriptor.fileFor(groupCompletionMarker));
    }

    public void createColumnCompletionMarker() throws IOException
    {
        Files.touch(descriptor.fileFor(columnCompletionMarker));
    }

    @Override
    public String toString()
    {
        return MoreObjects.toStringHelper(this).add("descriptor", descriptor)
                          .add("indexName", indexName)
                          .toString();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        IndexComponents components = (IndexComponents) o;

        if (descriptor != null ? !descriptor.equals(components.descriptor) : components.descriptor != null)
            return false;
        return indexName != null ? indexName.equals(components.indexName) : components.indexName == null;
    }

    @Override
    public int hashCode()
    {
        int result = descriptor != null ? descriptor.hashCode() : 0;
        result = 31 * result + (indexName != null ? indexName.hashCode() : 0);
        return result;
    }

    private void validatePerColumnComponents(boolean isLiteral, boolean checksum) throws IOException
    {
        MetadataSource source = MetadataSource.loadColumnMetadata(this);
        List<SegmentMetadata> segments = SegmentMetadata.load(source, null);

        for (IndexComponent component : perColumnComponents(indexName, isLiteral))
        {
            if (!component.ndiType.completionMarker())
            {
                if (component.ndiType.perSegment())
                {
                    for (int i = 0; i < segments.size(); i++)
                    {
                        SegmentMetadata metadata = segments.get(i);
                        boolean isLastSegment = i == segments.size() - 1;

                        validateSegment(component, metadata, isLastSegment, checksum, false);
                    }
                }
                else
                {
                    validateComponent(component, checksum);
                }
            }
        }
    }

    @SuppressWarnings("resource")
    private void validateSegment(IndexComponent component, SegmentMetadata metadata, boolean isLastSegment, boolean checksum, boolean isEncrypted) throws IOException
    {
        long offset = metadata.getIndexOffset(component);
        long length = metadata.getIndexLength(component);

        try (IndexInput input = openBlockingInput(component))
        {
            // Even if the component is encryptable, we still need to check for corruption if it isn't actually encrypted:
            if (!component.ndiType.encryptable() || !isEncrypted)
            {
                // Make sure there isn't any data appended incorrectly after the official end of the file:
                if (isLastSegment && input.length() != offset + length)
                {
                    String message = logMessage(String.format("Corrupted last segment! offset (%d) + length (%d) != file "+
                            "length (%s) isEncrypted: %s component.ndiType.encryptable: %s component.ndiType: %s",
                            offset, length, input.length(), isEncrypted, component.ndiType.encryptable(), component.ndiType.toString()));
                    throw new CorruptIndexException(message, descriptor.toString());
                }
            }

            IndexInput slice = input.slice(String.format("%s with offset=%d and length=%d]", input.toString(), offset, length), offset, length);

            if (checksum)
                SAICodecUtils.validateChecksum(slice);
            else
                SAICodecUtils.validate(slice);
        }
        catch (IOException e)
        {
            if (logger.isDebugEnabled())
            {
                logger.debug(logMessage("Per-segment {} validation failed for index component {} on SSTable {}"), (checksum ? "checksum " : ""), component, descriptor);
            }
            throw e;
        }
    }

    private void validateComponent(IndexComponent component, boolean checksum) throws IOException
    {
        if (!component.ndiType.completionMarker())
        {
            try (IndexInput input = openBlockingInput(component))
            {
                if (checksum)
                    SAICodecUtils.validateChecksum(input);
                else
                    SAICodecUtils.validate(input);
            }
            catch (IOException e)
            {
                if (logger.isDebugEnabled())
                {
                    logger.debug(logMessage("{} failed for index component {} on SSTable {}"), (checksum ? "Checksum validation" : "Validation"), component, descriptor);
                }
                throw e;
            }
        }
    }


    interface ChecksumWriter
    {
        long getChecksum();
    }

//    class EncryptedIncrementalChecksumSequentialWriter extends EncryptedSequentialWriter implements ChecksumWriter
//    {
//        private final CRC32 checksum = new CRC32();
//
//        EncryptedIncrementalChecksumSequentialWriter(File file, ICompressor encryptor)
//        {
//            super(file, writerOption, encryptor);
//        }
//
//        @Override
//        public void writeByte(int b) throws IOException
//        {
//            super.writeByte(b);
//            checksum.update(b);
//        }
//
//        @Override
//        public void write(byte[] b) throws IOException
//        {
//            super.write(b);
//            checksum.update(b);
//        }
//
//        @Override
//        public void write(byte[] b, int off, int len) throws IOException
//        {
//            super.write(b, off, len);
//            checksum.update(b, off, len);
//        }
//
//        public long getChecksum()
//        {
//            return checksum.getValue();
//        }
//    }

    class IncrementalChecksumSequentialWriter extends SequentialWriter implements ChecksumWriter
    {
        private final CRC32 checksum = new CRC32();

        IncrementalChecksumSequentialWriter(File file)
        {
            super(file, writerOption);
        }

        @Override
        public void writeByte(int b) throws IOException
        {
            super.writeByte(b);
            checksum.update(b);
        }

        @Override
        public void write(byte[] b) throws IOException
        {
            super.write(b);
            checksum.update(b);
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException
        {
            super.write(b, off, len);
            checksum.update(b, off, len);
        }

        public long getChecksum()
        {
            return checksum.getValue();
        }
    }
}
