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

package org.apache.cassandra.metrics;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.apache.cassandra.config.CassandraRelevantProperties.CUSTOM_CLIENT_REQUEST_METRICS_PROVIDER_PROPERTY;
import static org.assertj.core.api.Assertions.assertThat;

public class CustomCoordinatorMetricsTest
{
    @BeforeClass
    public static void beforeClass()
    {
        // Sets custom client provider class used in {@code CoordinatorClientRequestMetricsProvider#instance}
        CUSTOM_CLIENT_REQUEST_METRICS_PROVIDER_PROPERTY.setString(CoordinatorClientRequestMetricsProvider.DefaultCoordinatorMetricsProvider.class.getName());
    }

    @AfterClass
    public static void teardown()
    {
        CUSTOM_CLIENT_REQUEST_METRICS_PROVIDER_PROPERTY.setString("");
    }

    @Test
    public void testStaticInstanceWithCustomProviderClassName()
    {
        // Custom client provider class name set in {@link beforeClass()}
        CoordinatorClientRequestMetricsProvider customClientRequestMetricsProvider = CoordinatorClientRequestMetricsProvider.instance;
        assertThat(customClientRequestMetricsProvider).isInstanceOf(CoordinatorClientRequestMetricsProvider.DefaultCoordinatorMetricsProvider.class);
        CoordinatorClientRequestMetrics metrics = customClientRequestMetricsProvider.metrics("");
    }

    @Test
    public void testMakeProviderWithClassThatExists()
    {
        CoordinatorClientRequestMetricsProvider.make(CoordinatorClientRequestMetricsProvider.DefaultCoordinatorMetricsProvider.class.getName());
    }

    @Test(expected = IllegalStateException.class)
    public void testMakeProviderWithClassThatDoesNotExist()
    {
        CoordinatorClientRequestMetricsProvider.make("SomeOtherCLass");
    }
}
