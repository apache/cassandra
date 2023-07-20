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

import com.amazon.corretto.crypto.provider.AmazonCorrettoCryptoProvider;

public class DefaultCryptoProvider implements ICryptoProvider
{
    private static final Logger logger = LoggerFactory.getLogger(DefaultCryptoProvider.class);

    public DefaultCryptoProvider(Map<String, String> args)
    {
    }

    @Override
    public void installProvider()
    {
        try
        {
            AmazonCorrettoCryptoProvider.install();
        }
        catch (Exception e)
        {
            logger.warn("The installation of {} was not successful.", AmazonCorrettoCryptoProvider.class.getName(), e);
        }
    }

    @Override
    public void checkProvider() throws Exception
    {
        try
        {
            String currentCryptoProvider = Cipher.getInstance("AES/GCM/NoPadding").getProvider().getName();

            if (AmazonCorrettoCryptoProvider.PROVIDER_NAME.equals(currentCryptoProvider))
            {
                AmazonCorrettoCryptoProvider.INSTANCE.assertHealthy();
                logger.info("{} successfully passed the healthiness check", AmazonCorrettoCryptoProvider.PROVIDER_NAME);
            }
            else
            {
                logger.warn("{} is not the highest priority provider - {} is used. " +
                            "The most probable cause is that Cassandra node is not running on the same architecture " +
                            "the Amazon Corretto Crypto Provider library is for." +
                            "Please place the architecture-specific library for {} to the classpath and try again. ",
                            AmazonCorrettoCryptoProvider.PROVIDER_NAME,
                            currentCryptoProvider,
                            AmazonCorrettoCryptoProvider.class.getName());
            }
        }
        catch (Exception e)
        {
            logger.warn("Exception encountered while asserting the healthiness of " + AmazonCorrettoCryptoProvider.class.getName(), e);
        }
    }
}
