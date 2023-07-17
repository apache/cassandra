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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.exceptions.StartupException;
import com.amazon.corretto.crypto.provider.AmazonCorrettoCryptoProvider;

public class DefaultCryptoProvider implements ICryptoProvider
{
    private static final Logger logger = LoggerFactory.getLogger(DefaultCryptoProvider.class);

    public DefaultCryptoProvider(Map<String, String> args) {}
    @Override
    public void installProvider() throws StartupException
    {
        try
        {
            AmazonCorrettoCryptoProvider.install();
            AmazonCorrettoCryptoProvider.INSTANCE.assertHealthy();
        }
        catch(Exception e)
        {
            logger.warn("Amazon Corretto Crypto Provider is not available", e);
        }
    }

    @Override
    public void checkProvider() throws Exception
    {
        try
        {
            if (Cipher.getInstance("AES/GCM/NoPadding").getProvider().getName().equals(AmazonCorrettoCryptoProvider.PROVIDER_NAME))
            {
                AmazonCorrettoCryptoProvider.INSTANCE.assertHealthy();
            }
            else
            {
                logger.warn("{} is not the highest priority provider", AmazonCorrettoCryptoProvider.class.getName());
            }
        }
        catch (Exception e)
        {
            logger.warn("Corretto Crypto Provider Error", e);
        }
    }
}
