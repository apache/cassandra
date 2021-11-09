/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.locator;

import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.config.CassandraRelevantProperties.DYNAMIC_SNITCH_SEVERITY_PROVIDER;


/**
 * Class to abstract gossiper out of dynamic snitch
 */
public interface DynamicSnitchSeverityProvider
{
    DynamicSnitchSeverityProvider instance = DYNAMIC_SNITCH_SEVERITY_PROVIDER.getString() == null ?
                                             new DefaultProvider() : make(DYNAMIC_SNITCH_SEVERITY_PROVIDER.getString());

    /**
     * @return true if initialization is completed and ready to update dynamic snitch scores
     */
    boolean isReady();

    /**
     * update the severity for given endpoint
     *
     * @param endpoint endpoint to be updated
     * @param severity severity for the endpoint
     */
    void setSeverity(InetAddressAndPort endpoint, double severity);

    /**
     * @return severity for the endpoint or 0.0 if not found
     */
    double getSeverity(InetAddressAndPort endpoint);

    static DynamicSnitchSeverityProvider make(String customImpl)
    {
        try
        {
            return (DynamicSnitchSeverityProvider) Class.forName(customImpl).newInstance();
        }
        catch (Throwable ex)
        {
            throw new IllegalStateException("Unknown dynamic snitch severity provider: " + customImpl);
        }
    }

    class DefaultProvider implements DynamicSnitchSeverityProvider
    {
        @Override
        public boolean isReady()
        {
            // to resolve circular initializer dependency deadlock from CASSANDRA-1756
            return StorageService.instance.isGossipActive();
        }

        @Override
        public void setSeverity(InetAddressAndPort endpoint, double severity)
        {
            if (!endpoint.equals(FBUtilities.getBroadcastAddressAndPort()))
                throw new UnsupportedOperationException("Default severity provider only supports setting local severity, but got " + endpoint);

            Gossiper.instance.addLocalApplicationState(ApplicationState.SEVERITY, StorageService.instance.valueFactory.severity(severity));
        }

        @Override
        public double getSeverity(InetAddressAndPort endpoint)
        {
            EndpointState state = Gossiper.instance.getEndpointStateForEndpoint(endpoint);
            if (state == null)
                return 0.0;

            VersionedValue event = state.getApplicationState(ApplicationState.SEVERITY);
            if (event == null)
                return 0.0;

            return Double.parseDouble(event.value);
        }
    }
}
