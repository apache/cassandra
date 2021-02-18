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

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.cassandra.io.compress.CompressionMetadata;
import org.apache.cassandra.io.compress.ICompressor;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.CompressionParams;
import org.apache.lucene.store.ByteArrayIndexInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;

public class CryptoUtils
{

    public static CompressionMetadata getCompressionMeta(SSTableReader ssTableReader)
    {
        return ssTableReader.compression ? ssTableReader.getCompressionMetadata() : null;
    }

    public static CompressionParams getCompressionParams(SSTableReader ssTableReader)
    {
        return getCompressionParams(getCompressionMeta(ssTableReader));
    }

    public static CompressionParams getCompressionParams(CompressionMetadata meta)
    {
        return meta != null ? meta.parameters : null;
    }

    //TODO Encryption tidyup
//    public static ICompressor getEncryptionCompressor(CompressionParams compressionParams)
//    {
//        ICompressor compressor = compressionParams != null ? compressionParams.getSstableCompressor() : null;
//        return compressor != null ? compressor.encryptionOnly() : null;
//    }
//
//    public static boolean isCryptoEnabled(CompressionParams params)
//    {
//        ICompressor sstableCompressor = params != null ? params.getSstableCompressor() : null;
//        return sstableCompressor != null && sstableCompressor.encryptionOnly() != null ? true : false;
//    }

    public static IndexInput uncompress(IndexInput input, ICompressor compressor) throws IOException
    {
        return uncompress(input, compressor,
                          new BytesRef(new byte[16]), new BytesRef(new byte[16])
        );
    }

    /**
     * Takes an {@link IndexInput} with compressed/encrypted data and returns another {@link IndexInput} with
     * that data uncompressed/decrypted.
     */
    public static IndexInput uncompress(IndexInput input, ICompressor compressor, BytesRef compBytes, BytesRef uncompBytes) throws IOException
    {
        final int uncompBytesLen = input.readVInt();
        final int compBytesLength = input.readVInt();

        assert compBytesLength > 0 : "uncompBytesLen="+uncompBytesLen+" compBytesLength="+compBytesLength;

        compBytes.bytes = ArrayUtil.grow(compBytes.bytes, compBytesLength);

        input.readBytes(compBytes.bytes, 0, compBytesLength);

        if (uncompBytes.bytes == BytesRef.EMPTY_BYTES)
        {
            // if EMPTY_BYTES use an exact new byte array
            uncompBytes.bytes = new byte[uncompBytesLen];
            uncompBytes.length = uncompBytesLen;
        }
        else
        {
            uncompBytes.bytes = ArrayUtil.grow(uncompBytes.bytes, uncompBytesLen);
            uncompBytes.length = uncompBytesLen;
        }
        compressor.uncompress(compBytes.bytes, 0, compBytesLength, uncompBytes.bytes, 0);

        return new ByteArrayIndexInput("", uncompBytes.bytes, 0, uncompBytesLen);
    }

    public static void compress(BytesRef uncompBytes,
                                IndexOutput out, ICompressor compressor) throws IOException
    {
       compress(uncompBytes, new BytesRef(new byte[16]), out, compressor);
    }

    public static void compress(BytesRef uncompBytes, BytesRef compBytes,
                                IndexOutput out, ICompressor compressor) throws IOException
    {
        ByteBuffer input = ByteBuffer.wrap(uncompBytes.bytes, 0, uncompBytes.length);

        final int initCompLen = compressor.initialCompressedBufferLength(uncompBytes.length);

        compBytes.bytes = ArrayUtil.grow(compBytes.bytes, initCompLen);
        compBytes.length = initCompLen;

        ByteBuffer output = ByteBuffer.wrap(compBytes.bytes);

        compressor.compress(input, output);

        final int compLen = output.position();

        compBytes.length = compLen;

        assert uncompBytes.length > 0;
        assert compLen > 0;

        out.writeVInt(uncompBytes.length);
        out.writeVInt(compLen);

        out.writeBytes(compBytes.bytes, compLen);
    }
}
