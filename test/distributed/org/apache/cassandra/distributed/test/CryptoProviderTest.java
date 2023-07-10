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

package org.apache.cassandra.distributed.test;

import java.security.Provider;
import java.security.Security;
import java.util.HashMap;

import org.junit.Before;
import org.junit.Test;

import com.amazon.corretto.crypto.provider.AmazonCorrettoCryptoProvider;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IIsolatedExecutor.SerializableCallable;
import org.apache.cassandra.security.DefaultCryptoProvider;
import org.apache.cassandra.security.JREProvider;

import static java.lang.String.format;
import static net.bytebuddy.matcher.ElementMatchers.named;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class CryptoProviderTest extends TestBaseImpl
{
    @Before
    public void beforeTest()
    {
        assertNotInstalledDefaultProvider();
    }

    @Test
    public void testDefaultCryptoProvider() throws Throwable
    {
        try (Cluster cluster = init(Cluster.build(1)
                                           .withConfig(config -> config.set("crypto_provider",
                                                                            new HashMap<>()
                                                                            {{
                                                                                put("class_name", DefaultCryptoProvider.class.getName());
                                                                            }}))
                                           .start()))
        {
            assertTrue("Amazon Corretto Crypto Provider should be installed!",
                       cluster.get(1).callOnInstance((SerializableCallable<Boolean>) () -> {
                           Provider provider = Security.getProviders()[0];
                           return provider != null && AmazonCorrettoCryptoProvider.PROVIDER_NAME.equals(provider.getName());
                       }));
        }
        finally
        {
            Security.removeProvider(AmazonCorrettoCryptoProvider.PROVIDER_NAME);
        }
    }

    @Test
    public void testUsingJREProvider() throws Throwable
    {
        try (Cluster cluster = init(Cluster.build(1)
                                           .withConfig(config -> config.set("crypto_provider",
                                                                            new HashMap<>()
                                                                            {{
                                                                                put("class_name", JREProvider.class.getName());
                                                                            }}))
                                           .start()))
        {
            assertTrue("Amazon Corretto Crypto Provider should not be installed!",
                       cluster.get(1).callOnInstance((SerializableCallable<Boolean>) () -> {
                           Provider provider = Security.getProviders()[0];
                           return provider != null && !AmazonCorrettoCryptoProvider.PROVIDER_NAME.equals(provider.getName());
                       }));
        }
        finally
        {
            Security.removeProvider(AmazonCorrettoCryptoProvider.PROVIDER_NAME);
        }
    }

    @Test
    public void testUsingNoProviderUsesJREProvider() throws Throwable
    {
        // crypto_provider = null will be in Descriptor if it is not set in yaml (e.g. nodes being upgraded
        // with the old cassandra.yaml, or if it is commented out
        try (Cluster cluster = init(Cluster.build(1).start()))
        {
            assertTrue("Amazon Corretto Crypto Provider should not be installed!",
                       cluster.get(1).callOnInstance((SerializableCallable<Boolean>) () -> {
                           Provider provider = Security.getProviders()[0];
                           return provider != null && !AmazonCorrettoCryptoProvider.PROVIDER_NAME.equals(provider.getName());
                       }));
        }
        finally
        {
            Security.removeProvider(AmazonCorrettoCryptoProvider.PROVIDER_NAME);
        }
    }

    @Test
    public void testFailedDefaultProviderInstallationShouldResumeStartup() throws Throwable
    {
        try (Cluster cluster = builder().withNodes(1)
                                        .withInstanceInitializer(BB::install)
                                        .withConfig(config -> config.set("crypto_provider",
                                                                         new HashMap<>()
                                                                         {{
                                                                             put("class_name", DefaultCryptoProvider.class.getName());
                                                                         }}))
                                        .createWithoutStarting())
        {
            cluster.get(1).startup();

            assertTrue("Amazon Corretto Crypto Provider should not be installed!",
                       cluster.get(1).callOnInstance((SerializableCallable<Boolean>) () -> {
                           Provider provider = Security.getProviders()[0];
                           return provider != null && !AmazonCorrettoCryptoProvider.PROVIDER_NAME.equals(provider.getName());
                       }));
        }
        catch (Exception ex)
        {
            fail("Startup should not fail! It should fallback to in-built providers and continue to boot");
        }
        finally
        {
            Security.removeProvider(AmazonCorrettoCryptoProvider.PROVIDER_NAME);
        }
    }

    @Test
    public void testFailedDefaultProviderInstallationShouldFailStartupOnFailOnMissingProperty() throws Throwable
    {
        try (Cluster cluster = builder().withNodes(1)
                                        .withInstanceInitializer(BB::install)
                                        .withConfig(config -> config.set("crypto_provider",
                                                                         new HashMap<>()
                                                                         {{
                                                                             put("class_name", DefaultCryptoProvider.class.getName());
                                                                             put("parameters", new HashMap<>()
                                                                             {{
                                                                                 put("fail_on_missing_provider", "true");
                                                                             }});
                                                                         }}))
                                        .createWithoutStarting())
        {
            cluster.get(1).startup();

            fail("Startup should fail! It should not fallback to in-built providers and continue to boot " +
                 "as fail_on_missing_provider is set to true");
        }
        catch (Exception ex)
        {
            assertEquals(format("The installation of %s was not successful, reason: exception from test", AmazonCorrettoCryptoProvider.class.getName()),
                         ex.getMessage());

            assertNotInstalledDefaultProvider();
        }
        finally
        {
            Security.removeProvider(AmazonCorrettoCryptoProvider.PROVIDER_NAME);
        }
    }

    public static void assertNotInstalledDefaultProvider()
    {
        for (Provider provider : Security.getProviders())
            assertNotEquals(AmazonCorrettoCryptoProvider.PROVIDER_NAME, provider.getName());
    }

    public static class BB
    {
        public static void install(ClassLoader cl, Integer num)
        {
            new ByteBuddy().redefine(DefaultCryptoProvider.class)
                           .method(named("installator"))
                           .intercept(MethodDelegation.to(BB.class))
                           .make()
                           .load(cl, ClassLoadingStrategy.Default.INJECTION);
        }

        public static Runnable installator()
        {
            throw new RuntimeException("exception from test");
        }
    }
}
