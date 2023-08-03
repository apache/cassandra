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
import org.apache.cassandra.utils.Pair;

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
 */
public class Ec2Snitch extends AbstractCloudMetadataServiceSnitch
{
    private static final String SNITCH_PROP_NAMING_SCHEME = "ec2_naming_scheme";
    static final String EC2_NAMING_LEGACY = "legacy";
    private static final String EC2_NAMING_STANDARD = "standard";

    @VisibleForTesting
    public static final String ZONE_NAME_QUERY = "/latest/meta-data/placement/availability-zone";

    private final boolean usingLegacyNaming;

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
        super(connector, getDcAndRack(connector));
        usingLegacyNaming = isUsingLegacyNaming(connector.getProperties());
    }

    private static Pair<String, String> getDcAndRack(AbstractCloudMetadataServiceConnector connector) throws IOException
    {
        String az = connector.apiCall(ZONE_NAME_QUERY);

        // if using the full naming scheme, region name is created by removing letters from the
        // end of the availability zone and zone is the full zone name
        boolean usingLegacyNaming = isUsingLegacyNaming(connector.getProperties());
        String region;
        String localDc;
        String localRack;
        if (usingLegacyNaming)
        {
            // Split "us-east-1a" or "asia-1a" into "us-east"/"1a" and "asia"/"1a".
            String[] splits = az.split("-");
            localRack = splits[splits.length - 1];

            // hack for CASSANDRA-4026
            region = az.substring(0, az.length() - 1);
            if (region.endsWith("1"))
                region = az.substring(0, az.length() - 3);
        }
        else
        {
            // grab the region name, which is embedded in the availability zone name.
            // thus an AZ of "us-east-1a" yields the region name "us-east-1"
            region = az.replaceFirst("[a-z]+$","");
            localRack = az;
        }

        localDc = region.concat(connector.getProperties().getDcSuffix());

        return Pair.create(localDc, localRack);
    }

    private static boolean isUsingLegacyNaming(SnitchProperties props)
    {
        return props.get(SNITCH_PROP_NAMING_SCHEME, EC2_NAMING_STANDARD).equalsIgnoreCase(EC2_NAMING_LEGACY);
    }

    @Override
    public boolean validate(Set<String> datacenters, Set<String> racks)
    {
        return validate(datacenters, racks, usingLegacyNaming);
    }

    @VisibleForTesting
    static boolean validate(Set<String> datacenters, Set<String> racks, boolean usingLegacyNaming)
    {
        boolean valid = true;

        for (String dc : datacenters)
        {
            // predicated on the late-2017 AWS naming 'convention' that all region names end with a digit.
            // Unfortunately, life isn't that simple. Since we allow custom datacenter suffixes (CASSANDRA-5155),
            // an operator could conceiveably make the suffix "a", and thus create a region name that looks just like
            // one of the region's availability zones. (for example, "us-east-1a").
            // Further, we can't make any assumptions of what that suffix might be by looking at this node's
            // datacenterSuffix as conceivably their could be many different suffixes in play for a given region.
            //
            // It is impossible to distinguish standard and legacy names for datacenters in some cases
            // as the format didn't change for some regions (us-west-2 for example).
            // We can still identify as legacy the dc names without a number as a suffix like us-east"
            boolean dcUsesLegacyFormat = dc.matches("^[a-z]+-[a-z]+$");
            if (dcUsesLegacyFormat && !usingLegacyNaming)
            {
                valid = false;
                break;
            }
        }

        for (String rack : racks)
        {
            // predicated on late-2017 AWS naming 'convention' that AZs do not have a digit as the first char -
            // we had that in our legacy AZ (rack) names. Thus we test to see if the rack is in the legacy format.
            //
            // NOTE: the allowed custom suffix only applies to datacenter (region) names, not availability zones.
            boolean rackUsesLegacyFormat = rack.matches("[\\d][a-z]");
            if (rackUsesLegacyFormat != usingLegacyNaming)
            {
                valid = false;
                break;
            }
        }

        if (!valid)
        {
            logger.error("This ec2-enabled snitch appears to be using the {} naming scheme for regions, " +
                         "but existing nodes in cluster are using the opposite: region(s) = {}, availability zone(s) = {}. " +
                         "Please check the {} property in the {} configuration file for more details.",
                         usingLegacyNaming ? "legacy" : "standard", datacenters, racks,
                         SNITCH_PROP_NAMING_SCHEME, SnitchProperties.RACKDC_PROPERTY_FILENAME);
        }

        return valid;
    }
}
