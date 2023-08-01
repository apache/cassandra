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

import java.nio.file.Files;
import java.io.InputStream;
import java.io.IOException;
import java.security.Key;
import java.security.KeyStore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.TransparentDataEncryptionOptions;
import org.apache.cassandra.io.util.File;

/**
 * A {@code KeyProvider} that retrieves keys from a java keystore.
 */
public class JKSKeyProvider implements KeyProvider
{
    private static final Logger logger = LoggerFactory.getLogger(JKSKeyProvider.class);
    static final String PROP_KEYSTORE = "keystore";
    static final String PROP_KEYSTORE_PW = "keystore_password";
    static final String PROP_KEYSTORE_TYPE = "store_type";
    static final String PROP_KEY_PW = "key_password";

    private final KeyStore store;
    private final boolean isJceks;
    private final TransparentDataEncryptionOptions options;

    public JKSKeyProvider(TransparentDataEncryptionOptions options)
    {
        this.options = options;
        logger.info("initializing keystore from file {}", options.get(PROP_KEYSTORE));
        try (InputStream inputStream = Files.newInputStream(File.getPath(options.get(PROP_KEYSTORE))))
        {
            store = KeyStore.getInstance(options.get(PROP_KEYSTORE_TYPE));
            store.load(inputStream, options.get(PROP_KEYSTORE_PW).toCharArray());
            isJceks = store.getType().equalsIgnoreCase("jceks");
        }
        catch (Exception e)
        {
            throw new RuntimeException("couldn't load keystore", e);
        }
    }

    public Key getSecretKey(String keyAlias) throws IOException
    {
        // there's a lovely behavior with jceks files that all aliases are lower-cased
        if (isJceks)
            keyAlias = keyAlias.toLowerCase();

        Key key;
        try
        {
            String password = options.get(PROP_KEY_PW);
            if (password == null || password.isEmpty())
                password = options.get(PROP_KEYSTORE_PW);
            key = store.getKey(keyAlias, password.toCharArray());
        }
        catch (Exception e)
        {
            throw new IOException("unable to load key from keystore");
        }
        if (key == null)
            throw new IOException(String.format("key %s was not found in keystore", keyAlias));
        return key;
    }
}
