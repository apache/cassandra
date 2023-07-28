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

import java.util.Map;
import javax.crypto.Cipher;

import com.amazon.corretto.crypto.provider.AmazonCorrettoCryptoProvider;

/**
 * Default crypto provider tries to install AmazonCorrettoCryptoProvider.
 * <p>
 * The implementation falls back to in-built crypto provider in JRE if the installation
 * is not successful.
 */
public class DefaultCryptoProvider extends AbstractCryptoProvider
{
    public DefaultCryptoProvider(Map<String, String> args)
    {
        super(args);
    }

    @Override
    public String getProviderName()
    {
        return "AmazonCorrettoCryptoProvider";
    }

    @Override
    public String getProviderClassAsString()
    {
        return "com.amazon.corretto.crypto.provider.AmazonCorrettoCryptoProvider";
    }

    @Override
    public Runnable installator()
    {
        return AmazonCorrettoCryptoProvider::install;
    }

    @Override
    public boolean isHealthyInstallation() throws Exception
    {
        if (!getProviderName().equals(Cipher.getInstance("AES/GCM/NoPadding").getProvider().getName()))
            return false;

        AmazonCorrettoCryptoProvider.INSTANCE.assertHealthy();

        return true;
    }
}
