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

import java.security.Provider;
import java.security.Security;
import java.util.HashMap;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.amazon.corretto.crypto.provider.AmazonCorrettoCryptoProvider;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.ParameterizedClass;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.utils.FBUtilities;
import org.mockito.MockedStatic;

import static com.google.common.collect.ImmutableMap.of;
import static java.lang.String.format;
import static org.apache.cassandra.config.CassandraRelevantProperties.CRYPTO_PROVIDER_CLASS_NAME;
import static org.apache.cassandra.config.CassandraRelevantProperties.FAIL_ON_MISSING_CRYPTO_PROVIDER;
import static org.apache.cassandra.config.CassandraRelevantProperties.TEST_SKIP_CRYPTO_PROVIDER_INSTALLATION;
import static org.apache.cassandra.security.AbstractCryptoProvider.FAIL_ON_MISSING_PROVIDER_KEY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.spy;

public class CryptoProviderTest
{
    @BeforeClass
    public static void beforeClass()
    {
        TEST_SKIP_CRYPTO_PROVIDER_INSTALLATION.setBoolean(true);
        DatabaseDescriptor.daemonInitialization();
        TEST_SKIP_CRYPTO_PROVIDER_INSTALLATION.setBoolean(false);
    }

    @Before
    public void beforeTest()
    {
        // be sure it is uninstalled / reset
        DatabaseDescriptor.getRawConfig().crypto_provider = null;
        DatabaseDescriptor.setCryptoProvider(null);
        Security.removeProvider(AmazonCorrettoCryptoProvider.PROVIDER_NAME);
        assertNotInstalledDefaultProvider();
    }

    public static void assertNotInstalledDefaultProvider()
    {
        for (Provider provider : Security.getProviders())
            assertNotEquals(AmazonCorrettoCryptoProvider.PROVIDER_NAME, provider.getName());
    }

    @Test
    public void testCryptoProviderClassSystemProperty()
    {
        try (WithProperties properties = new WithProperties().set(CRYPTO_PROVIDER_CLASS_NAME, TestJREProvider.class.getName()))
        {
            DatabaseDescriptor.applyCryptoProvider();
            assertEquals(TestJREProvider.class.getSimpleName(), DatabaseDescriptor.getCryptoProvider().getProviderName());
        }
    }

    @Test
    public void testNoCryptoProviderInstallationUseJREProvider()
    {
        DatabaseDescriptor.applyCryptoProvider();
        assertEquals("JREProvider", DatabaseDescriptor.getCryptoProvider().getProviderName());
    }

    @Test
    public void testCryptoProviderInstallationWithNullParameters()
    {
        DatabaseDescriptor.getRawConfig().crypto_provider = new ParameterizedClass(TestJREProvider.class.getName(), null);
        DatabaseDescriptor.applyCryptoProvider();

        AbstractCryptoProvider cryptoProvider = DatabaseDescriptor.getCryptoProvider();
        assertThat(cryptoProvider.getProviderName()).isEqualTo(TestJREProvider.class.getSimpleName());
        assertThat(cryptoProvider.getProperties()).isNotNull()
                                                  .isNotEmpty()
                                                  .hasSize(1)
                                                  .containsKeys("fail_on_missing_provider")
                                                  .containsValues("false");
    }

    @Test
    public void testCryptoProviderInstallationWithEmptyParameters()
    {
        DatabaseDescriptor.getRawConfig().crypto_provider = new ParameterizedClass(TestJREProvider.class.getName(), of());
        DatabaseDescriptor.applyCryptoProvider();

        AbstractCryptoProvider cryptoProvider = DatabaseDescriptor.getCryptoProvider();
        assertThat(cryptoProvider.getProviderName()).isEqualTo(TestJREProvider.class.getSimpleName());
        assertThat(cryptoProvider.getProperties()).isNotNull()
                                                  .isNotEmpty()
                                                  .hasSize(1)
                                                  .containsKeys("fail_on_missing_provider")
                                                  .containsValues("false");
    }

    @Test
    public void testCryptoProviderInstallationWithNotEmptyParameters()
    {
        DatabaseDescriptor.getRawConfig().crypto_provider = new ParameterizedClass(TestJREProvider.class.getName(),
                                                                                   of("k1", "v1", "k2", "v2"));
        DatabaseDescriptor.applyCryptoProvider();

        AbstractCryptoProvider cryptoProvider = DatabaseDescriptor.getCryptoProvider();
        assertThat(cryptoProvider.getProviderName()).isEqualTo(TestJREProvider.class.getSimpleName());
        assertThat(cryptoProvider.getProperties()).isNotNull()
                                                  .isNotEmpty()
                                                  .hasSize(3)
                                                  .containsKeys("k1", "k2", "fail_on_missing_provider")
                                                  .containsValues("v1", "v2", "false");
    }

    @Test
    public void testCryptoProviderInstallationWithSimpleClassName()
    {
        DatabaseDescriptor.getRawConfig().crypto_provider = new ParameterizedClass(TestJREProvider.class.getSimpleName(), null);
        DatabaseDescriptor.applyCryptoProvider();

        AbstractCryptoProvider cryptoProvider = DatabaseDescriptor.getCryptoProvider();
        assertThat(cryptoProvider.getProviderName()).isEqualTo(TestJREProvider.class.getSimpleName());
        assertThat(cryptoProvider.getProperties()).isNotNull()
                                                  .isNotEmpty()
                                                  .hasSize(1)
                                                  .containsKeys("fail_on_missing_provider")
                                                  .containsValues("false");
    }

    @Test
    public void testUnableToCreateDefaultCryptoProvider()
    {
        try (MockedStatic<FBUtilities> fbUtilitiesMock = mockStatic(FBUtilities.class))
        {
            DatabaseDescriptor.getRawConfig().crypto_provider = new ParameterizedClass(DefaultCryptoProvider.class.getName(),
                                                                                       of("k1", "v1", "k2", "v2"));

            fbUtilitiesMock.when(() -> FBUtilities.classForName(DefaultCryptoProvider.class.getName(), "crypto provider class"))
                           .thenThrow(new RuntimeException("exception from test"));

            fbUtilitiesMock.when(() -> FBUtilities.newCryptoProvider(anyString(), anyMap())).thenCallRealMethod();

            assertThatThrownBy(DatabaseDescriptor::applyCryptoProvider)
            .isInstanceOf(ConfigurationException.class)
            .hasCauseInstanceOf(RuntimeException.class)
            .hasMessage("Unable to create an instance of crypto provider for " + DefaultCryptoProvider.class.getName())
            .hasRootCauseMessage("exception from test");

            assertNotInstalledDefaultProvider();
        }
    }

    @Test
    public void testFailOnMissingProviderSystemProperty()
    {
        try (WithProperties properties = new WithProperties().set(FAIL_ON_MISSING_CRYPTO_PROVIDER, "true")
                                                             .set(CRYPTO_PROVIDER_CLASS_NAME, InvalidCryptoProvider.class.getName()))
        {
            assertThatExceptionOfType(ConfigurationException.class)
            .isThrownBy(DatabaseDescriptor::applyCryptoProvider)
            .withMessage("some.package.non.existing.ClassName is not on the class path! Check node's architecture " +
                         "(`uname -m`) is supported, see lib/<arch> subdirectories. The correct architecture-specific " +
                         "library for needs to be on the classpath.");
        }
    }

    @Test
    public void testCryptoProviderInstallation() throws Exception
    {
        AbstractCryptoProvider provider = new DefaultCryptoProvider(new HashMap<>());
        assertFalse(provider.failOnMissingProvider);

        Provider originalProvider = Security.getProviders()[0];

        provider.install();
        assertTrue(provider.isHealthyInstallation());
        Provider installedProvider = Security.getProviders()[0];
        assertEquals(installedProvider.getName(), provider.getProviderName());

        provider.uninstall();
        Provider currentProvider = Security.getProviders()[0];
        assertNotEquals(currentProvider.getName(), installedProvider.getName());
        assertEquals(originalProvider.getName(), currentProvider.getName());
    }

    @Test
    public void testInvalidProviderInstallator()
    {
        AbstractCryptoProvider spiedProvider = spy(new DefaultCryptoProvider(of(FAIL_ON_MISSING_PROVIDER_KEY, "true")));

        Runnable installator = () ->
        {
            throw new RuntimeException("invalid installator");
        };

        doReturn(installator).when(spiedProvider).installator();

        assertThatExceptionOfType(ConfigurationException.class)
        .isThrownBy(spiedProvider::install)
        .withRootCauseInstanceOf(RuntimeException.class)
        .withMessage("The installation of %s was not successful, reason: invalid installator", spiedProvider.getProviderClassAsString());
    }

    @Test
    public void testNullInstallatorThrowsException()
    {
        AbstractCryptoProvider spiedProvider = spy(new DefaultCryptoProvider(of(FAIL_ON_MISSING_PROVIDER_KEY, "true")));

        doReturn(null).when(spiedProvider).installator();

        assertThatExceptionOfType(ConfigurationException.class)
        .isThrownBy(spiedProvider::install)
        .withRootCauseInstanceOf(RuntimeException.class)
        .withMessage("The installation of %s was not successful, reason: Installator runnable can not be null!", spiedProvider.getProviderClassAsString());
    }

    @Test
    public void testProviderHealthcheckerReturningFalse() throws Exception
    {
        AbstractCryptoProvider spiedProvider = spy(new DefaultCryptoProvider(of(FAIL_ON_MISSING_PROVIDER_KEY, "true")));

        doReturn(false).when(spiedProvider).isHealthyInstallation();

        assertThatExceptionOfType(ConfigurationException.class)
        .isThrownBy(spiedProvider::install)
        .withCause(null)
        .withMessage(format("%s has not passed the health check. " +
                            "Check node's architecture (`uname -m`) is supported, see lib/<arch> subdirectories. " +
                            "The correct architecture-specific library for %s needs to be on the classpath. ",
                            spiedProvider.getProviderName(),
                            spiedProvider.getProviderClassAsString()));
    }

    @Test
    public void testHealthcheckerThrowingException() throws Exception
    {
        AbstractCryptoProvider spiedProvider = spy(new DefaultCryptoProvider(of(FAIL_ON_MISSING_PROVIDER_KEY, "true")));

        Throwable t = new RuntimeException("error in health checker");
        doThrow(t).when(spiedProvider).isHealthyInstallation();

        assertThatExceptionOfType(ConfigurationException.class)
        .isThrownBy(spiedProvider::install)
        .withCauseInstanceOf(RuntimeException.class)
        .withMessage(format("The installation of %s was not successful, reason: %s",
                            spiedProvider.getProviderClassAsString(), t.getMessage()));
    }

    @Test
    public void testProviderNotOnClassPathWithPropertyInYaml() throws Exception
    {
        InvalidCryptoProvider cryptoProvider = new InvalidCryptoProvider(of(FAIL_ON_MISSING_PROVIDER_KEY, "true"));

        assertThatExceptionOfType(ConfigurationException.class)
        .isThrownBy(cryptoProvider::install)
        .withMessage("some.package.non.existing.ClassName is not on the class path! Check node's architecture " +
                     "(`uname -m`) is supported, see lib/<arch> subdirectories. " +
                     "The correct architecture-specific library for needs to be on the classpath.");
    }

    @Test
    public void testProviderNotOnClassPathWithSystemProperty()
    {
        try (WithProperties properties = new WithProperties().set(FAIL_ON_MISSING_CRYPTO_PROVIDER, "true"))
        {
            InvalidCryptoProvider cryptoProvider = new InvalidCryptoProvider(of());

            assertThatExceptionOfType(ConfigurationException.class)
            .isThrownBy(cryptoProvider::install)
            .withMessage("some.package.non.existing.ClassName is not on the class path! Check node's architecture " +
                         "(`uname -m`) is supported, see lib/<arch> subdirectories. The correct architecture-specific " +
                         "library for needs to be on the classpath.");
        }
    }

    @Test
    public void testProviderInstallsJustOnce() throws Exception
    {
        Provider[] originalProviders = Security.getProviders();
        int originalProvidersCount = originalProviders.length;
        Provider originalProvider = Security.getProviders()[0];

        AbstractCryptoProvider provider = new DefaultCryptoProvider(new HashMap<>());
        provider.install();

        assertEquals(provider.getProviderName(), Security.getProviders()[0].getName());
        assertEquals(originalProvidersCount + 1, Security.getProviders().length);

        // install one more time -> it will do nothing

        provider.install();

        assertEquals(provider.getProviderName(), Security.getProviders()[0].getName());
        assertEquals(originalProvidersCount + 1, Security.getProviders().length);

        provider.uninstall();

        assertEquals(originalProvider.getName(), Security.getProviders()[0].getName());
        assertEquals(originalProvidersCount, Security.getProviders().length);
    }

    @Test
    public void testInstallationOfIJREProvider() throws Exception
    {
        String originalProvider = Security.getProviders()[0].getName();

        JREProvider jreProvider = new JREProvider(of());
        jreProvider.install();

        assertEquals(originalProvider, Security.getProviders()[0].getName());
    }
}
