package org.apache.cassandra.locator;

import java.io.IOException;
import java.net.InetAddress;

import javax.xml.parsers.ParserConfigurationException;

import org.junit.Test;
import org.xml.sax.SAXException;

public class DatacenterStrategyTest
{
    private String table = "Keyspace1";

	@Test
    public void testProperties() throws IOException, ParserConfigurationException, SAXException
    {
    	XMLFileSnitch snitch = new XMLFileSnitch();
    	TokenMetadata metadata = new TokenMetadata();
    	InetAddress localhost = InetAddress.getLocalHost();
    	// Set the localhost to the tokenmetadata. Embeded cassandra way?
    	// metadata.addBootstrapToken();
        DatacenterShardStrategy strategy = new DatacenterShardStrategy(new TokenMetadata(), snitch);
        assert strategy.getReplicationFactor("dc1", table) == 3;
        assert strategy.getReplicationFactor("dc2", table) == 5;
        assert strategy.getReplicationFactor("dc3", table) == 1;
        // Query for the natural hosts
        // strategy.getNaturalEndpoints(token, table)
    }

}
