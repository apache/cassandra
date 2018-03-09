/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.cassandra.security;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;

import com.google.common.base.Charsets;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.config.TransparentDataEncryptionOptions;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

public class CipherFactoryTest
{
    // http://www.gutenberg.org/files/4300/4300-h/4300-h.htm
    static final String ULYSSEUS = "Stately, plump Buck Mulligan came from the stairhead, bearing a bowl of lather on which a mirror and a razor lay crossed. " +
                                   "A yellow dressinggown, ungirdled, was sustained gently behind him on the mild morning air. He held the bowl aloft and intoned: " +
                                   "-Introibo ad altare Dei.";
    TransparentDataEncryptionOptions encryptionOptions;
    CipherFactory cipherFactory;
    SecureRandom secureRandom;

    @Before
    public void setup()
    {
        try
        {
            secureRandom = SecureRandom.getInstance("SHA1PRNG");
            assertNotNull(secureRandom.getProvider());
        }
        catch (NoSuchAlgorithmException e)
        {
            fail("NoSuchAlgorithmException: SHA1PRNG not found.");
        }
        long seed = new java.util.Random().nextLong();
        System.out.println("Seed: " + seed);
        secureRandom.setSeed(seed);
        encryptionOptions = EncryptionContextGenerator.createEncryptionOptions();
        cipherFactory = new CipherFactory(encryptionOptions);
    }

    @Test
    public void roundTrip() throws IOException, BadPaddingException, IllegalBlockSizeException
    {
        Cipher encryptor = cipherFactory.getEncryptor(encryptionOptions.cipher, encryptionOptions.key_alias);
        byte[] original = ULYSSEUS.getBytes(Charsets.UTF_8);
        byte[] encrypted = encryptor.doFinal(original);

        Cipher decryptor = cipherFactory.getDecryptor(encryptionOptions.cipher, encryptionOptions.key_alias, encryptor.getIV());
        byte[] decrypted = decryptor.doFinal(encrypted);
        Assert.assertEquals(ULYSSEUS, new String(decrypted, Charsets.UTF_8));
    }

    private byte[] nextIV()
    {
        byte[] b = new byte[16];
        secureRandom.nextBytes(b);
        return b;
    }

    @Test
    public void buildCipher_SameParams() throws Exception
    {
        byte[] iv = nextIV();
        Cipher c1 = cipherFactory.buildCipher(encryptionOptions.cipher, encryptionOptions.key_alias, iv, Cipher.ENCRYPT_MODE);
        Cipher c2 = cipherFactory.buildCipher(encryptionOptions.cipher, encryptionOptions.key_alias, iv, Cipher.ENCRYPT_MODE);
        Assert.assertTrue(c1 == c2);
    }

    @Test
    public void buildCipher_DifferentModes() throws Exception
    {
        byte[] iv = nextIV();
        Cipher c1 = cipherFactory.buildCipher(encryptionOptions.cipher, encryptionOptions.key_alias, iv, Cipher.ENCRYPT_MODE);
        Cipher c2 = cipherFactory.buildCipher(encryptionOptions.cipher, encryptionOptions.key_alias, iv, Cipher.DECRYPT_MODE);
        Assert.assertFalse(c1 == c2);
    }

    @Test
    public void buildCipher_DifferentIVs() throws Exception
    {
        Cipher c1 = cipherFactory.buildCipher(encryptionOptions.cipher, encryptionOptions.key_alias, nextIV(), Cipher.ENCRYPT_MODE);
        Cipher c2 = cipherFactory.buildCipher(encryptionOptions.cipher, encryptionOptions.key_alias, nextIV(), Cipher.DECRYPT_MODE);
        Assert.assertFalse(c1 == c2);
    }

    @Test
    public void buildCipher_DifferentAliases() throws Exception
    {
        Cipher c1 = cipherFactory.buildCipher(encryptionOptions.cipher, encryptionOptions.key_alias, nextIV(), Cipher.ENCRYPT_MODE);
        Cipher c2 = cipherFactory.buildCipher(encryptionOptions.cipher, EncryptionContextGenerator.KEY_ALIAS_2, nextIV(), Cipher.DECRYPT_MODE);
        Assert.assertFalse(c1 == c2);
    }

    @Test(expected = AssertionError.class)
    public void getDecryptor_NullIv() throws IOException
    {
        cipherFactory.getDecryptor(encryptionOptions.cipher, encryptionOptions.key_alias, null);
    }

    @Test(expected = AssertionError.class)
    public void getDecryptor_EmptyIv() throws IOException
    {
        cipherFactory.getDecryptor(encryptionOptions.cipher, encryptionOptions.key_alias, new byte[0]);
    }
}
