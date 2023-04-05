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

import java.io.IOException;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.config.TransparentDataEncryptionOptions;

public class JKSKeyProviderTest
{
    JKSKeyProvider jksKeyProvider;
    TransparentDataEncryptionOptions tdeOptions;

    @Before
    public void setup()
    {
        tdeOptions = EncryptionContextGenerator.createEncryptionOptions();
        jksKeyProvider = new JKSKeyProvider(tdeOptions);
    }

    @Test
    public void getSecretKey_WithKeyPassword() throws IOException
    {
        Assert.assertNotNull(jksKeyProvider.getSecretKey(tdeOptions.key_alias));
    }

    @Test
    public void getSecretKey_WithoutKeyPassword() throws IOException
    {
        tdeOptions.remove("key_password");
        Assert.assertNotNull(jksKeyProvider.getSecretKey(tdeOptions.key_alias));
    }
}
