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

package org.apache.cassandra.locator;

import static org.apache.cassandra.config.CassandraRelevantProperties.CUSTOM_TMD_PROVIDER_PROPERTY;

import org.junit.Test;

import junit.framework.TestCase;

public class CustomTokenMetadataProviderTest extends TestCase
{
    public static class TestTokenMetadataProvider implements TokenMetadataProvider
    {
        @Override
        public TokenMetadata getTokenMetadata()
        {
            return null;
        }

        @Override
        public void replaceTokenMetadata(TokenMetadata newTokenMetadata)
        {
        }
    }

    @Test
    public void testMake()
    {
        final String customImpl = TestTokenMetadataProvider.class.getName();
        CustomTokenMetadataProvider.make(customImpl);
    }

    @Test
    public void testInvalidTokenMetadataClassThrows()
    {
        final String invalidClassName = "invalidClass";
        try
        {
            CustomTokenMetadataProvider.make(invalidClassName);
            fail();
        }
        catch (IllegalStateException ex)
        {
            assertEquals(ex.getMessage(), "Unknown token metadata provider: " + invalidClassName);
        }
    }

    @Test
    public void testCustomTokenMetadataProperty() throws ClassNotFoundException
    {
        String oldValue = CUSTOM_TMD_PROVIDER_PROPERTY.getString();
        CUSTOM_TMD_PROVIDER_PROPERTY.setString(TestTokenMetadataProvider.class.getName());
        ClassLoader classLoader = CustomTokenMetadataProvider.class.getClassLoader();
        try
        {
            classLoader.loadClass("org.apache.cassandra.locator.TokenMetadataProvider");
            boolean instanceIsExpectedClass = TokenMetadataProvider.instance instanceof TestTokenMetadataProvider;
            assertTrue("TokenMetadataProvider has unexpected instance class", instanceIsExpectedClass);
        }
        finally
        {
            if (oldValue != null)
            {
                CUSTOM_TMD_PROVIDER_PROPERTY.setString(oldValue);
            }
        }
    }
}