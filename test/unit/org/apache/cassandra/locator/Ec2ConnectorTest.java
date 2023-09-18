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

import java.time.Duration;
import java.util.Properties;

import org.junit.Test;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.locator.Ec2MetadataServiceConnector.EC2MetadataType;
import org.apache.cassandra.locator.Ec2MetadataServiceConnector.V1Connector;
import org.apache.cassandra.locator.Ec2MetadataServiceConnector.V2Connector;

import static java.lang.String.format;
import static java.util.Arrays.stream;
import static java.util.stream.Collectors.joining;
import static org.apache.cassandra.locator.Ec2MetadataServiceConnector.DEFAULT_EC2_METADATA_URL;
import static org.apache.cassandra.locator.Ec2MetadataServiceConnector.EC2_METADATA_TYPE_PROPERTY;
import static org.apache.cassandra.locator.Ec2MetadataServiceConnector.V2Connector.AWS_EC2_METADATA_TOKEN_TTL_SECONDS_HEADER_PROPERTY;
import static org.apache.cassandra.locator.Ec2MetadataServiceConnector.V2Connector.MAX_TOKEN_TIME_IN_SECONDS;
import static org.apache.cassandra.locator.Ec2MetadataServiceConnector.V2Connector.MIN_TOKEN_TIME_IN_SECONDS;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class Ec2ConnectorTest
{
    @Test
    public void testV1Configuration()
    {
        Properties p = new Properties();
        p.setProperty(EC2_METADATA_TYPE_PROPERTY, EC2MetadataType.v1.name());
        Ec2MetadataServiceConnector ec2Connector = Ec2MetadataServiceConnector.create(new SnitchProperties(p));

        assertTrue(ec2Connector instanceof V1Connector);

        assertEquals(DEFAULT_EC2_METADATA_URL, ec2Connector.metadataServiceUrl);
    }

    @Test
    public void testV2Configuration()
    {
        Ec2MetadataServiceConnector ec2Connector = Ec2MetadataServiceConnector.create(new SnitchProperties(new Properties()));

        // v2 connector by default
        assertTrue(ec2Connector instanceof V2Connector);
        assertEquals(DEFAULT_EC2_METADATA_URL, ec2Connector.metadataServiceUrl);
        assertEquals(Duration.ofSeconds(MAX_TOKEN_TIME_IN_SECONDS), ((V2Connector) ec2Connector).tokenTTL);
    }

    @Test
    public void testInvalidConfiguration()
    {
        assertThatExceptionOfType(ConfigurationException.class)
        .describedAs("it should not be possible create a connector of type non-existent")
        .isThrownBy(() -> {
            Properties p = new Properties();
            p.setProperty(EC2_METADATA_TYPE_PROPERTY, "non-existent");
            Ec2MetadataServiceConnector.create(new SnitchProperties(p));
        }).withMessage(format("%s must be one of %s", EC2_METADATA_TYPE_PROPERTY,
                              stream(EC2MetadataType.values()).map(Enum::name).collect(joining(", "))));

        assertThatExceptionOfType(ConfigurationException.class)
        .describedAs(format("it should not be possible to set %s to more than %s",
                            AWS_EC2_METADATA_TOKEN_TTL_SECONDS_HEADER_PROPERTY,
                            MAX_TOKEN_TIME_IN_SECONDS))
        .isThrownBy(() -> {
            Properties p = new Properties();
            p.setProperty(EC2_METADATA_TYPE_PROPERTY, EC2MetadataType.v2.name());
            p.setProperty(AWS_EC2_METADATA_TOKEN_TTL_SECONDS_HEADER_PROPERTY, "50000");

            Ec2MetadataServiceConnector.create(new SnitchProperties(p));
        }).withMessage(format("property %s was set to 50000 seconds which is not in allowed range of [%s..%s]",
                              AWS_EC2_METADATA_TOKEN_TTL_SECONDS_HEADER_PROPERTY,
                              MIN_TOKEN_TIME_IN_SECONDS,
                              MAX_TOKEN_TIME_IN_SECONDS));

        assertThatExceptionOfType(ConfigurationException.class)
        .describedAs("ttl value should be integer")
        .isThrownBy(() -> {
            Properties p = new Properties();
            p.setProperty(EC2_METADATA_TYPE_PROPERTY, EC2MetadataType.v2.name());
            p.setProperty(AWS_EC2_METADATA_TOKEN_TTL_SECONDS_HEADER_PROPERTY, "abc");

            Ec2MetadataServiceConnector.create(new SnitchProperties(p));
        }).withMessage(format("Unable to parse integer from property %s, value to parse: abc",
                              AWS_EC2_METADATA_TOKEN_TTL_SECONDS_HEADER_PROPERTY));
    }
}
