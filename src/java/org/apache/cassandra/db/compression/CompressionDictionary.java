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

import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;

import org.apache.cassandra.cql3.UntypedResultSet;

public interface CompressionDictionary extends AutoCloseable
{
    /**
     * Get the dictionary id
     *
     * @return dictionary id
     */
    DictId identifier();

    /**
     * Get the raw bytes of the compression dictionary
     *
     * @return raw compression dictionary
     */
    byte[] rawDictionary();

    /**
     * Get the kind of the compression algorithm
     *
     * @return compression algorithm kind
     */
    default Kind kind()
    {
        return identifier().kind;
    }

    /**
     * Write compression dictionary to file
     *
     * @param out file output stream
     * @throws IOException on any I/O exception when writing to the file
     */
    default void serialize(DataOutput out) throws IOException
    {
        DictId dictId = identifier();
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
        {
            throw new IOException("Compression dictionary checksum does not match");
        }

        CompressionDictionary dictionary = null;
        if (kind == Kind.ZSTD)
        {
            dictionary = new ZstdCompressionDictionary(dictId, dict);
        }

        if (dictionary == null)
        {
            throw new IOException(kind + " compression dictionary is not created");
        }

        // update the dictionary manager if it exists
        if (manager != null)
        {
            manager.add(dictionary);
        }
        return dictionary;
    }

    static CompressionDictionary createFromRow(UntypedResultSet.Row row)
    {
        String kindStr = row.getString("kind");
        long id = row.getLong("dict_id");
        byte[] dict = row.getByteArray("dict");
        CompressionDictionary.DictId dictId = new CompressionDictionary.DictId(CompressionDictionary.Kind.valueOf(kindStr), id);
        if (dictId.kind == CompressionDictionary.Kind.ZSTD)
        {
            return new ZstdCompressionDictionary(dictId, dict);
        }

        throw new IllegalStateException(kindStr + " compression dictionary is not created");
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

    enum Kind
    {
        // Order matters: the enum ordinal is serialized
        ZSTD;
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
}
