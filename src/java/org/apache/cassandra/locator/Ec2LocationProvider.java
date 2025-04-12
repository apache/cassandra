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
import java.util.HashSet;
import java.util.Set;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.membership.Location;

public class Ec2LocationProvider extends CloudMetadataLocationProvider
{
    static final String SNITCH_PROP_NAMING_SCHEME = "ec2_naming_scheme";
    static final String EC2_NAMING_LEGACY = "legacy";
    static final String EC2_NAMING_STANDARD = "standard";

    @VisibleForTesting
    public static final String ZONE_NAME_QUERY = "/latest/meta-data/placement/availability-zone";

    private final boolean usingLegacyNaming;

    /**
     * Used via reflection by DatabaseDescriptor::createInitialLocationProvider
     */
    public Ec2LocationProvider() throws IOException, ConfigurationException
    {
        this(new SnitchProperties());
    }

    public Ec2LocationProvider(SnitchProperties props) throws IOException, ConfigurationException
    {
        this(Ec2MetadataServiceConnector.create(props));
    }

    Ec2LocationProvider(AbstractCloudMetadataServiceConnector connector) throws IOException
    {
        super(connector, Ec2LocationProvider::getLocation);
        usingLegacyNaming = isUsingLegacyNaming(connector.getProperties());
    }

    private static Location getLocation(AbstractCloudMetadataServiceConnector connector) throws IOException
    {
        // if using the full naming scheme, region name is created by removing letters from the
        // end of the availability zone and zone is the full zone name
        boolean usingLegacyNaming = isUsingLegacyNaming(connector.getProperties());

        String az = connector.apiCall(ZONE_NAME_QUERY);
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

        return new Location(localDc, localRack);
    }

    private static boolean isUsingLegacyNaming(SnitchProperties props)
    {
        return props.get(SNITCH_PROP_NAMING_SCHEME, EC2_NAMING_STANDARD).equalsIgnoreCase(EC2_NAMING_LEGACY);
    }

    public void validate(ClusterMetadata metadata)
    {
        // Validate that the settings here match what was used to locate
        // and register other nodes in the cluster (if there are any)
        Set<String> datacenters = metadata.directory.allDatacenterRacks().keySet();
        Set<String> racks = new HashSet<>();
        for (String dc : datacenters)
            racks.addAll(metadata.directory.datacenterRacks(dc).keySet());
        validate(datacenters, racks);
    }

    public boolean validate(Set<String> datacenters, Set<String> racks)
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
            throw new ConfigurationException(String.format("This ec2-enabled location provider appears to be using the " +
                                                           "%s naming scheme for regions, but existing nodes in cluster " +
                                                           "are using the opposite: " +
                                                           "region(s) = %s, availability zone(s) = %s. " +
                                                           "Please check the %s property in the %s configuration file " +
                                                           "for more details.",
                                                           usingLegacyNaming ? "legacy" : "standard", datacenters, racks,
                                                           SNITCH_PROP_NAMING_SCHEME, SnitchProperties.RACKDC_PROPERTY_FILENAME));
        }
        return true;
    }
}
