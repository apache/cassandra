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
package org.apache.cassandra.hints;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.zip.CRC32;
import javax.crypto.Cipher;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.io.util.File;
import org.apache.cassandra.security.EncryptionUtils;
import org.apache.cassandra.io.compress.ICompressor;

import static org.apache.cassandra.utils.FBUtilities.updateChecksum;

public class EncryptedHintsWriter extends HintsWriter
{
    private final Cipher cipher;
    private final ICompressor compressor;
    private volatile ByteBuffer byteBuffer;

    protected EncryptedHintsWriter(File directory, HintsDescriptor descriptor, File file, FileChannel channel, int fd, CRC32 globalCRC)
    {
        super(directory, descriptor, file, channel, fd, globalCRC);
        cipher = descriptor.getCipher();
        compressor = descriptor.createCompressor();
    }

    protected void writeBuffer(ByteBuffer input) throws IOException
    {
        byteBuffer = EncryptionUtils.compress(input, byteBuffer, true, compressor);
        ByteBuffer output = EncryptionUtils.encryptAndWrite(byteBuffer, channel, true, cipher);
        updateChecksum(globalCRC, output);
    }

    @VisibleForTesting
    Cipher getCipher()
    {
        return cipher;
    }

    @VisibleForTesting
    ICompressor getCompressor()
    {
        return compressor;
    }
}
