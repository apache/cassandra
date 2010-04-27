package org.apache.cassandra.locator;

import org.junit.Test;

import org.apache.cassandra.config.ConfigurationException;

public class DatacenterStrategyTest
{
    @Test
    public void testProperties() throws ConfigurationException
    {
        DatacenterShardStrategy strategy = new DatacenterShardStrategy(new TokenMetadata(), new RackInferringSnitch());
        assert strategy.getReplicationFactor("dc1") == 3;
        assert strategy.getReplicationFactor("dc2") == 5;
        assert strategy.getReplicationFactor("dc3") == 1;
    }

}
