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

import java.util.HashMap;
import java.util.Map;

import org.apache.cassandra.config.ParameterizedClass;
import org.apache.cassandra.config.TransparentDataEncryptionOptions;

public class EncryptionContextGenerator
{
    public static final String KEY_ALIAS_1 = "testing:1";
    public static final String KEY_ALIAS_2 = "testing:2";

    public static EncryptionContext createContext(boolean init)
    {
        return createContext(null, init);
    }

    public static EncryptionContext createContext(byte[] iv, boolean init)
    {
        return new EncryptionContext(createEncryptionOptions(), iv, init);
    }

    public static TransparentDataEncryptionOptions createEncryptionOptions()
    {
        Map<String,String> params = new HashMap<>();
        params.put("keystore", "test/conf/cassandra.keystore");
        params.put("keystore_password", "cassandra");
        params.put("store_type", "JCEKS");
        ParameterizedClass keyProvider = new ParameterizedClass(JKSKeyProvider.class.getName(), params);

        return new TransparentDataEncryptionOptions("AES/CBC/PKCS5Padding", KEY_ALIAS_1, keyProvider);
    }

    public static EncryptionContext createDisabledContext()
    {
        return new EncryptionContext();
    }
}
