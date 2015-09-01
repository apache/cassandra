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
package org.apache.cassandra.security;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.Random;
import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.ShortBufferException;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.config.TransparentDataEncryptionOptions;
import org.apache.cassandra.io.compress.ICompressor;
import org.apache.cassandra.io.compress.LZ4Compressor;
import org.apache.cassandra.io.util.RandomAccessReader;

public class EncryptionUtilsTest
{
    final Random random = new Random();
    ICompressor compressor;
    TransparentDataEncryptionOptions tdeOptions;

    @Before
    public void setup()
    {
        compressor = LZ4Compressor.create(new HashMap<>());
        tdeOptions = EncryptionContextGenerator.createEncryptionOptions();
    }

    @Test
    public void compress() throws IOException
    {
        byte[] buf = new byte[(1 << 13) - 13];
        random.nextBytes(buf);
        ByteBuffer compressedBuffer = EncryptionUtils.compress(ByteBuffer.wrap(buf), ByteBuffer.allocate(0), true, compressor);
        ByteBuffer uncompressedBuffer = EncryptionUtils.uncompress(compressedBuffer, ByteBuffer.allocate(0), true, compressor);
        Assert.assertArrayEquals(buf, uncompressedBuffer.array());
    }

    @Test
    public void encrypt() throws BadPaddingException, ShortBufferException, IllegalBlockSizeException, IOException
    {
        byte[] buf = new byte[(1 << 12) - 7];
        random.nextBytes(buf);

        // encrypt
        CipherFactory cipherFactory = new CipherFactory(tdeOptions);
        Cipher encryptor = cipherFactory.getEncryptor(tdeOptions.cipher, tdeOptions.key_alias);

        File f = File.createTempFile("commitlog-enc-utils-", ".tmp");
        f.deleteOnExit();
        FileChannel channel = new RandomAccessFile(f, "rw").getChannel();
        EncryptionUtils.encryptAndWrite(ByteBuffer.wrap(buf), channel, true, encryptor);
        channel.close();

        // decrypt
        Cipher decryptor = cipherFactory.getDecryptor(tdeOptions.cipher, tdeOptions.key_alias, encryptor.getIV());
        ByteBuffer decryptedBuffer = EncryptionUtils.decrypt(RandomAccessReader.open(f), ByteBuffer.allocate(0), true, decryptor);

        // normally, we'd just call BB.array(), but that gives you the *entire* backing array, not with any of the offsets (position,limit) applied.
        // thus, just for this test, we copy the array and perform an array-level comparison with those offsets
        decryptedBuffer.limit(buf.length);
        byte[] b = new byte[buf.length];
        System.arraycopy(decryptedBuffer.array(), 0, b, 0, buf.length);
        Assert.assertArrayEquals(buf, b);
    }

    @Test
    public void fullRoundTrip() throws IOException, BadPaddingException, ShortBufferException, IllegalBlockSizeException
    {
        // compress
        byte[] buf = new byte[(1 << 12) - 7];
        random.nextBytes(buf);
        ByteBuffer compressedBuffer = EncryptionUtils.compress(ByteBuffer.wrap(buf), ByteBuffer.allocate(0), true, compressor);

        // encrypt
        CipherFactory cipherFactory = new CipherFactory(tdeOptions);
        Cipher encryptor = cipherFactory.getEncryptor(tdeOptions.cipher, tdeOptions.key_alias);
        File f = File.createTempFile("commitlog-enc-utils-", ".tmp");
        f.deleteOnExit();
        FileChannel channel = new RandomAccessFile(f, "rw").getChannel();
        EncryptionUtils.encryptAndWrite(compressedBuffer, channel, true, encryptor);

        // decrypt
        Cipher decryptor = cipherFactory.getDecryptor(tdeOptions.cipher, tdeOptions.key_alias, encryptor.getIV());
        ByteBuffer decryptedBuffer = EncryptionUtils.decrypt(RandomAccessReader.open(f), ByteBuffer.allocate(0), true, decryptor);

        // uncompress
        ByteBuffer uncompressedBuffer = EncryptionUtils.uncompress(decryptedBuffer, ByteBuffer.allocate(0), true, compressor);
        Assert.assertArrayEquals(buf, uncompressedBuffer.array());
    }
}
