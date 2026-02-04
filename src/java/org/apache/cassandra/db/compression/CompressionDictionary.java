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

package org.apache.cassandra.db.compression;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.EOFException;
import java.io.IOException;
import java.util.Objects;

import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;

import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.io.compress.ICompressor;
import org.apache.cassandra.io.compress.ZstdDictionaryCompressor;
import org.apache.cassandra.utils.concurrent.Ref;

/**
 * Interface for compression dictionaries with opt-in reference-counted lifecycle management.
 * <p>
 * Dictionaries can be used in two modes:
 * <ul>
 *   <li><b>Lightweight mode</b>: No native resources allocated. Suitable for export, serialization,
 *       or scenarios where only the raw dictionary bytes are needed.</li>
 *   <li><b>Managed mode</b>: Native compression/decompression resources allocated on-demand and
 *       managed via reference counting. Required for caching and active use.</li>
 * </ul>
 * Call {@link #initRefLazily()} or {@link #tryRef()} to transition from lightweight to managed mode.
 *
 * <h2>Reference Counting Model</h2>
 * Compression dictionaries hold native resources that must be explicitly managed. This interface
 * uses {@link Ref} for safe lifecycle management across multiple concurrent users.
 *
 * <h3>Ownership and Usage in Cassandra</h3>
 * <ul>
 *   <li><b>CompressionDictionaryManager</b>: Holds the primary reference ({@link #selfRef()}) for cached dictionaries</li>
 *   <li><b>CompressionMetadata.Writer</b>: Acquires a reference during SSTable write, held for the writer's lifetime</li>
 *   <li><b>CompressionMetadata</b>: Acquires a reference when created (via {@link #tryRef()}), held for the SSTable reader's lifetime.
 *       All copies created via sharedCopy() share this single reference through WrappedSharedCloseable</li>
 * </ul>
 *
 * <h3>Correctness Guarantee</h3>
 * The reference counting prevents premature cleanup of native resources:
 * <ol>
 *   <li>CompressionMetadata acquires a reference when an SSTable is opened</li>
 *   <li>Native resources remain valid as long as any reference exists (refcount &gt; 0)</li>
 *   <li>Even if the cache evicts the dictionary, the SSTable's reference keeps resources alive</li>
 *   <li>Cleanup runs exactly once when the last reference is released (refcount goes 0 → -1)</li>
 *   <li>After cleanup, {@link #tryRef()} returns null, preventing new references to released resources</li>
 * </ol>
 *
 * This ensures dictionaries cannot be freed while SSTables are using them for compression/decompression,
 * even when the cache evicts the dictionary concurrently.
 *
 * @see Ref for reference counting implementation
 * @see CompressionDictionaryManager for cache management
 * @see org.apache.cassandra.io.compress.CompressionMetadata for SSTable usage
 */
public interface CompressionDictionary
{
    /**
     * Get the dictionary id
     *
     * @return dictionary id
     */
    DictId dictId();

    /**
     * Get the raw bytes of the compression dictionary
     *
     * @return raw compression dictionary
     */
    byte[] rawDictionary();

    /**
     * Get checksum of this dictionary.
     *
     * @return checksum of this dictionary
     */
    int checksum();

    /**
     * Get memory occupied of this dictionary as a whole.
     * Use for metrics exposing used memory in total. The value
     * return by this method is a best-effort estimation.
     *
     * @return memory occuppied by this compression dictionary, in bytes
     */
    int estimatedOccupiedMemoryBytes();

    /**
     * Get the kind of the compression algorithm
     *
     * @return compression algorithm kind
     */
    default Kind kind()
    {
        return dictId().kind;
    }

    /**
     * Returns a reference from lazily initialized reference counter.
     *
     * @return reference to this dictionary; once initialized, the reference is the same as self-reference
     */
    Ref<? extends CompressionDictionary> initRefLazily();

    /**
     * Try to acquire a new reference to this dictionary.
     * Returns null if the dictionary is already released.
     * <p>
     * The caller must ensure the returned reference is released when no longer needed,
     * either by calling {@code ref.release()} or {@code ref.close()} (they are equivalent).
     * Failing to release the reference will prevent cleanup of native resources and cause
     * a memory leak.
     *
     * @return a new reference to this dictionary, or null if already released
     */
    Ref<? extends CompressionDictionary> tryRef();

    /**
     * Get the self-reference of this dictionary.
     * This is used to release the primary reference held by the cache.
     * Self-reference is initialized after initRefLazily or tryRef
     *
     * @return the self-reference or null if not yet initialized.
     */
    @Nullable
    Ref<? extends CompressionDictionary> selfRef();

    /**
     * Releases the self-reference of this dictionary.
     * This is a convenience method equivalent to calling {@code selfRef().close()}.
     * <p>
     * This method is idempotent - calling it multiple times is safe and will only
     * release the self-reference once. Subsequent calls have no effect.
     * <p>
     * There is no need to call this method in the context of releasing references when an instance
     * is never put to a cache. That might be the case when we are constructing a dictionary object
     * just for the purpose of passing it to a user (e.g. on exporting and similar) where reference counting
     * is not necessary nor needed.
     * <p>
     * For dictionaries managed by the cache, the cache's removal listener handles cleanup via
     * {@code selfRef().release()}.
     *
     * @see #selfRef()
     * @see #tryRef()
     */
    @VisibleForTesting
    default void close()
    {
        Ref<?> selfRef = selfRef();
        if (selfRef != null)
            selfRef.close();
    }

    /**
     * Write compression dictionary to file
     *
     * @param out file output stream
     * @throws IOException on any I/O exception when writing to the file
     */
    default void serialize(DataOutput out) throws IOException
    {
        DictId dictId = dictId();
        int ordinal = dictId.kind.ordinal();
        out.writeByte(ordinal);
        out.writeLong(dictId.id);
        byte[] dict = rawDictionary();
        out.writeInt(dict.length);
        out.write(dict);
        int checksum = calculateChecksum((byte) ordinal, dictId.id, dict);
        out.writeInt(checksum);
    }

    /**
     * A factory method to create concrete CompressionDictionary from the file content
     *
     * @param input   file input stream
     * @param manager compression dictionary manager that caches the dictionaries
     * @return compression dictionary; otherwise, null if there is no dictionary
     * @throws IOException on any I/O exception when reading from the file
     */
    @Nullable
    static CompressionDictionary deserialize(DataInput input, @Nullable CompressionDictionaryManager manager) throws IOException
    {
        int kindOrdinal;
        try
        {
            kindOrdinal = input.readByte();
        }
        catch (EOFException eof)
        {
            // no dictionary
            return null;
        }

        if (kindOrdinal < 0 || kindOrdinal >= Kind.values().length)
        {
            throw new IOException("Invalid compression dictionary kind: " + kindOrdinal);
        }
        Kind kind = Kind.values()[kindOrdinal];
        long id = input.readLong();
        DictId dictId = new DictId(kind, id);

        if (manager != null)
        {
            CompressionDictionary dictionary = manager.get(dictId);
            if (dictionary != null)
            {
                return dictionary;
            }
        }

        int length = input.readInt();
        byte[] dict = new byte[length];
        input.readFully(dict);
        int checksum = input.readInt();
        int calculatedChecksum = calculateChecksum((byte) kindOrdinal, id, dict);
        if (checksum != calculatedChecksum)
            throw new IOException("Compression dictionary checksum does not match. " +
                                  "Expected: " + checksum + "; actual: " + calculatedChecksum);

        CompressionDictionary dictionary = kind.createDictionary(dictId, dict, checksum);

        // update the dictionary manager if it exists
        if (manager != null)
        {
            manager.add(dictionary);
        }

        return dictionary;
    }

    static LightweightCompressionDictionary createFromRowLightweight(UntypedResultSet.Row row)
    {
        String kindStr = row.getString("kind");
        long dictId = row.getLong("dict_id");
        int checksum = row.getInt("dict_checksum");
        int size = row.getInt("dict_length");
        String keyspaceName = row.getString("keyspace_name");
        String tableName = row.getString("table_name");

        try
        {
            return new LightweightCompressionDictionary(keyspaceName,
                                                        tableName,
                                                        new DictId(CompressionDictionary.Kind.valueOf(kindStr), dictId),
                                                        checksum,
                                                        size);
        }
        catch (IllegalArgumentException ex)
        {
            throw new IllegalStateException(kindStr + " compression dictionary is not created for dict id " + dictId);
        }
    }

    static CompressionDictionary createFromRow(UntypedResultSet.Row row)
    {
        String kindStr = row.getString("kind");
        long dictId = row.getLong("dict_id");
        byte[] dict = row.getByteArray("dict");
        int storedLength = row.getInt("dict_length");
        int storedChecksum = row.getInt("dict_checksum");

        try
        {
            Kind kind = CompressionDictionary.Kind.valueOf(kindStr);

            // Validate length
            if (dict.length != storedLength)
            {
                throw new IllegalStateException(String.format("Dictionary length mismatch for %s dict id %d. Expected: %d, actual: %d",
                                                              kindStr, dictId, storedLength, dict.length));
            }

            // Validate checksum
            int calculatedChecksum = calculateChecksum((byte) kind.ordinal(), dictId, dict);
            if (calculatedChecksum != storedChecksum)
            {
                throw new IllegalStateException(String.format("Dictionary checksum mismatch for %s dict id %d. Expected: %d, actual: %d",
                                                              kindStr, dictId, storedChecksum, calculatedChecksum));
            }

            return kind.createDictionary(new DictId(kind, dictId), row.getByteArray("dict"), storedChecksum);
        }
        catch (IllegalArgumentException ex)
        {
            throw new IllegalStateException(kindStr + " compression dictionary is not created for dict id " + dictId);
        }
    }

    @SuppressWarnings("UnstableApiUsage")
    static int calculateChecksum(byte kindOrdinal, long dictId, byte[] dict)
    {
        Hasher hasher = Hashing.crc32c().newHasher();
        hasher.putByte(kindOrdinal);
        hasher.putLong(dictId);
        hasher.putBytes(dict);
        return hasher.hash().asInt();
    }

    // Defines the compression dictionary kind, as well as serves as a factory for creating components of dictionary compression
    enum Kind
    {
        // Order matters: the enum ordinal is serialized
        ZSTD
        {
            @Override
            public CompressionDictionary createDictionary(DictId dictId, byte[] dict, int checksum)
            {
                return new ZstdCompressionDictionary(dictId, dict, checksum);
            }

            @Override
            public ICompressor createCompressor(CompressionDictionary dictionary)
            {
                Preconditions.checkArgument(dictionary instanceof ZstdCompressionDictionary,
                                            "Expected dictionary to be ZstdCompressionDictionary; actual: %s",
                                            dictionary.getClass().getSimpleName());
                return ZstdDictionaryCompressor.create((ZstdCompressionDictionary) dictionary);
            }

            @Override
            public ICompressionDictionaryTrainer createTrainer(String keyspaceName,
                                                               String tableName,
                                                               ICompressor compressor)
            {
                Preconditions.checkArgument(compressor instanceof ZstdDictionaryCompressor,
                                            "Expected compressor to be ZstdDictionaryCompressor; actual: %s",
                                            compressor.getClass().getSimpleName());
                return new ZstdDictionaryTrainer(keyspaceName, tableName, ((ZstdDictionaryCompressor) compressor).compressionLevel());
            }
        };

        /**
         * Creates a compression dictionary instance for this kind
         *
         * @param dictId the dictionary identifier
         * @param dict the raw dictionary bytes
         * @param checksum checksum of this dictionary
         * @return a compression dictionary instance
         */
        public abstract CompressionDictionary createDictionary(CompressionDictionary.DictId dictId, byte[] dict, int checksum);

        /**
         * Creates a dictionary compressor for this kind
         *
         * @param dictionary the compression dictionary to use for compression
         * @return a dictionary compressor instance
         */
        public abstract ICompressor createCompressor(CompressionDictionary dictionary);

        /**
         * Creates a dictionary trainer for this kind
         *
         * @param keyspaceName the keyspace name
         * @param tableName the table name
         * @param compressor the compressor to use for training
         * @return a dictionary trainer instance
         */
        public abstract ICompressionDictionaryTrainer createTrainer(String keyspaceName,
                                                                    String tableName,
                                                                    ICompressor compressor);
    }

    final class DictId
    {
        public final Kind kind;
        public final long id; // A value of negative or 0 means no dictionary

        public DictId(Kind kind, long id)
        {
            this.kind = kind;
            this.id = id;
        }

        @Override
        public boolean equals(Object o)
        {
            if (!(o instanceof DictId)) return false;
            DictId dictId = (DictId) o;
            return id == dictId.id && kind == dictId.kind;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(kind, id);
        }

        @Override
        public String toString()
        {
            return "DictId{" +
                   "kind=" + kind +
                   ", id=" + id +
                   '}';
        }
    }

    /**
     * The purpose of lightweight dictionary is to not carry the actual dictionary bytes for performance reasons.
     * Handy for situations when retrieval from the database does not need to contain dictionary
     * or the instantiation of a proper dictionary object is not desirable or unnecessary for other,
     * mostly performance-related, reasons.
     */
    class LightweightCompressionDictionary
    {
        public final String keyspaceName;
        public final String tableName;
        public final DictId dictId;
        public final int checksum;
        public final int size;

        public LightweightCompressionDictionary(String keyspaceName,
                                                String tableName,
                                                DictId dictId,
                                                int checksum,
                                                int size)
        {
            this.keyspaceName = keyspaceName;
            this.tableName = tableName;
            this.dictId = dictId;
            this.checksum = checksum;
            this.size = size;
        }
    }
}
