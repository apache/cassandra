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

import java.io.BufferedReader;
import java.io.IOException;
import java.net.URI;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileReader;
import org.apache.cassandra.locator.AbstractCloudMetadataServiceConnector.DefaultCloudMetadataServiceConnector;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.Pair;

import static org.apache.cassandra.locator.AbstractCloudMetadataServiceConnector.METADATA_URL_PROPERTY;

/**
 * A snitch that assumes a Cloudstack Zone follows the typical convention
 * {@code country-location-availability zone} and uses the country/location
 * tuple as a datacenter and the availability zone as a rack
 *
 * This snitch is deprecated, and it is eligible for the removal in the next major release of Cassandra.
 *
 * @deprecated See CASSANDRA-18438
 */
@Deprecated(since = "5.0")
public class CloudstackSnitch extends AbstractCloudMetadataServiceSnitch
{
    static final String ZONE_NAME_QUERY_URI = "/latest/meta-data/availability-zone";

    private static final String[] LEASE_FILES =
    {
    "file:///var/lib/dhcp/dhclient.eth0.leases",
    "file:///var/lib/dhclient/dhclient.eth0.leases"
    };

    public CloudstackSnitch() throws IOException
    {
        this(new SnitchProperties(new Properties()));
    }

    public CloudstackSnitch(SnitchProperties snitchProperties) throws IOException
    {
        this(new DefaultCloudMetadataServiceConnector(snitchProperties.putIfAbsent(METADATA_URL_PROPERTY, csMetadataEndpoint())));
    }

    public CloudstackSnitch(AbstractCloudMetadataServiceConnector connector) throws IOException
    {
        super(connector, resolveDcAndRack(connector));
        logger.warn("{} is deprecated and not actively maintained. It will be removed in the next " +
                    "major version of Cassandra.", CloudstackSnitch.class.getName());
    }

    private static Pair<String, String> resolveDcAndRack(AbstractCloudMetadataServiceConnector connector) throws IOException
    {
        String zone = connector.apiCall(ZONE_NAME_QUERY_URI);
        String[] zoneParts = zone.split("-");

        if (zoneParts.length != 3)
            throw new ConfigurationException("CloudstackSnitch cannot handle invalid zone format: " + zone);

        return Pair.create(zoneParts[0] + '-' + zoneParts[1], zoneParts[2]);
    }

    private static String csMetadataEndpoint() throws ConfigurationException
    {
        for (String lease_uri : LEASE_FILES)
        {
            try
            {
                File lease_file = new File(new URI(lease_uri));
                if (lease_file.exists())
                {
                    return csEndpointFromLease(lease_file);
                }
            }
            catch (Exception e)
            {
                JVMStabilityInspector.inspectThrowable(e);
            }
        }

        throw new ConfigurationException("No valid DHCP lease file could be found.");
    }

    private static String csEndpointFromLease(File lease) throws ConfigurationException
    {
        String line;
        String endpoint = null;
        Pattern identifierPattern = Pattern.compile("^[ \t]*option dhcp-server-identifier (.*);$");

        try (BufferedReader reader = new BufferedReader(new FileReader(lease)))
        {

            while ((line = reader.readLine()) != null)
            {
                Matcher matcher = identifierPattern.matcher(line);

                if (matcher.find())
                {
                    endpoint = matcher.group(1);
                    break;
                }
            }
        }
        catch (Exception e)
        {
            throw new ConfigurationException("CloudstackSnitch cannot access lease file.");
        }

        if (endpoint == null)
        {
            throw new ConfigurationException("No metadata server could be found in lease file.");
        }

        return "http://" + endpoint;
    }
}
