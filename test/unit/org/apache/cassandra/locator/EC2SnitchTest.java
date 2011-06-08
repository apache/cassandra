package org.apache.cassandra.locator;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Map;

import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.service.StorageService;
import org.junit.Test;

public class EC2SnitchTest
{

    private class TestEC2Snitch extends Ec2Snitch
    {
        public TestEC2Snitch() throws IOException, ConfigurationException
        {
            super();
        }

        @Override
        String awsApiCall(String url) throws IOException, ConfigurationException
        {
            return "us-east-1d";
        }
    }

    @Test
    public void testRac() throws IOException, ConfigurationException
    {
        Ec2Snitch snitch = new TestEC2Snitch();
        InetAddress local = InetAddress.getByName("127.0.0.1");
        InetAddress nonlocal = InetAddress.getByName("127.0.0.7");
        
        Gossiper.instance.addSavedEndpoint(nonlocal);
        Map<ApplicationState,VersionedValue> stateMap = Gossiper.instance.getEndpointStateForEndpoint(nonlocal).getApplicationStateMap();
        stateMap.put(ApplicationState.DC, StorageService.instance.valueFactory.datacenter("us-west"));
        stateMap.put(ApplicationState.RACK, StorageService.instance.valueFactory.datacenter("1a"));
        
        assertEquals("us-west", snitch.getDatacenter(nonlocal));
        assertEquals("1a", snitch.getRack(nonlocal));
        
        assertEquals("us-east", snitch.getDatacenter(local));
        assertEquals("1d", snitch.getRack(local));
    }
}
