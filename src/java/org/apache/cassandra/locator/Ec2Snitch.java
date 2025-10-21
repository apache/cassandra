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

import java.io.IOException;
import java.util.Set;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.exceptions.ConfigurationException;

/**
 * A snitch that assumes an EC2 region is a DC and an EC2 availability_zone
 * is a rack. This information is available in the config for the node.

 * Since CASSANDRA-16555, it is possible to choose version of AWS IMDS.
 *
 * By default, since CASSANDRA-16555, IMDSv2 is used.
 *
 * The version of IMDS is driven by property {@link Ec2MetadataServiceConnector#EC2_METADATA_TYPE_PROPERTY} and
 * can be of value either 'v1' or 'v2'.
 *
 * It is possible to specify custom URL of IMDS by {@link Ec2MetadataServiceConnector#EC2_METADATA_URL_PROPERTY}.
 * A user is not meant to change this under normal circumstances, it is suitable for testing only.
 *
 * IMDSv2 is secured by a token which needs to be fetched from IDMSv2 first, and it has to be passed in a header
 * for the actual queries to IDMSv2. Ec2Snitch is doing this automatically. The only configuration parameter exposed
 * to a user is {@link Ec2MetadataServiceConnector.V2Connector#AWS_EC2_METADATA_TOKEN_TTL_SECONDS_HEADER_PROPERTY}
 * which is by default set to {@link Ec2MetadataServiceConnector.V2Connector#MAX_TOKEN_TIME_IN_SECONDS}. TTL has
 * to be an integer from the range [30, 21600].
 * @deprecated See CASSANDRA-19488
 */
@Deprecated(since = "CEP-21")
public class Ec2Snitch extends AbstractCloudMetadataServiceSnitch
{
    private static final String SNITCH_PROP_NAMING_SCHEME = "ec2_naming_scheme";
    static final String EC2_NAMING_LEGACY = "legacy";
    private static final String EC2_NAMING_STANDARD = "standard";

    @VisibleForTesting
    public static final String ZONE_NAME_QUERY = "/latest/meta-data/placement/availability-zone";

    public Ec2Snitch() throws IOException, ConfigurationException
    {
        this(new SnitchProperties());
    }

    public Ec2Snitch(SnitchProperties props) throws IOException, ConfigurationException
    {
        this(Ec2MetadataServiceConnector.create(props));
    }

    Ec2Snitch(AbstractCloudMetadataServiceConnector connector) throws IOException
    {
        super(new Ec2LocationProvider(connector));
    }

    @Override
    public boolean validate(Set<String> datacenters, Set<String> racks)
    {
        return ((Ec2LocationProvider)locationProvider).validate(datacenters, racks);
    }
}
