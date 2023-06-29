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

import org.junit.Test;

import org.apache.cassandra.config.DurationSpec;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.locator.AbstractCloudMetadataServiceConnector.DefaultCloudMetadataServiceConnector;
import org.apache.cassandra.utils.Pair;

import static org.apache.cassandra.locator.AbstractCloudMetadataServiceConnector.DEFAULT_METADATA_REQUEST_TIMEOUT;
import static org.apache.cassandra.locator.AbstractCloudMetadataServiceConnector.METADATA_REQUEST_TIMEOUT_PROPERTY;
import static org.apache.cassandra.locator.AbstractCloudMetadataServiceConnector.METADATA_URL_PROPERTY;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.junit.Assert.assertEquals;

public class DefaultCloudMetadataServiceConnectorTest
{
    @Test
    public void testDefaultConnector()
    {
        assertEquals(new DurationSpec.IntMillisecondsBound(DEFAULT_METADATA_REQUEST_TIMEOUT).toMilliseconds(),
                     new DefaultCloudMetadataServiceConnector(new SnitchProperties(Pair.create(METADATA_REQUEST_TIMEOUT_PROPERTY, DEFAULT_METADATA_REQUEST_TIMEOUT),
                                                                                   Pair.create(METADATA_URL_PROPERTY, "http://127.0.0.1/abc"))).requestTimeoutMs);

        assertEquals(0, new DefaultCloudMetadataServiceConnector(new SnitchProperties(Pair.create(METADATA_REQUEST_TIMEOUT_PROPERTY, "0s"), Pair.create(METADATA_URL_PROPERTY, "http://127.0.0.1/abc"))).requestTimeoutMs);
        assertEquals(30000, new DefaultCloudMetadataServiceConnector(new SnitchProperties(Pair.create(METADATA_URL_PROPERTY, "http://127.0.0.1/abc"))).requestTimeoutMs);
    }

    @Test
    public void testInvalidMetadataURL()
    {
        assertThatExceptionOfType(ConfigurationException.class)
        .isThrownBy(() -> new DefaultCloudMetadataServiceConnector(new SnitchProperties(Pair.create(METADATA_URL_PROPERTY, "http:"))))
        .withMessage("Snitch metadata service URL 'http:' is invalid. Please review snitch properties defined in the configured 'cassandra-rackdc.properties' configuration file.");
    }

    @Test
    public void testInvalidTimeout()
    {
        assertThatExceptionOfType(ConfigurationException.class)
        .isThrownBy(() -> new DefaultCloudMetadataServiceConnector(new SnitchProperties(Pair.create(METADATA_REQUEST_TIMEOUT_PROPERTY, "1x"),
                                                                                        Pair.create(METADATA_URL_PROPERTY, "http://127.0.0.1/abc"))))
        .withMessage("1x as value of metadata_request_timeout is invalid duration! Invalid duration: 1x Accepted units:[MILLISECONDS, SECONDS, MINUTES, HOURS, DAYS] where case matters and only non-negative values.");
    }
}
