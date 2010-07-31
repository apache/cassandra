package org.apache.cassandra.service;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.locator.AbstractRackAwareSnitch;
import org.apache.cassandra.locator.DatacenterShardStrategy;
import org.apache.cassandra.locator.RackInferringSnitch;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.utils.FBUtilities;

/**
 * Datacenter Quorum response handler blocks for a quorum of responses from the local DC
 */
public class DatacenterQuorumResponseHandler<T> extends QuorumResponseHandler<T>
{
    private static final AbstractRackAwareSnitch snitch = (AbstractRackAwareSnitch) DatabaseDescriptor.getEndpointSnitch();
	private static final String localdc = snitch.getDatacenter(FBUtilities.getLocalAddress());
    private AtomicInteger localResponses;
    
    public DatacenterQuorumResponseHandler(IResponseResolver<T> responseResolver, ConsistencyLevel consistencyLevel, String table)
    {
        super(responseResolver, consistencyLevel, table);
        localResponses = new AtomicInteger(blockfor);
    }
    
    @Override
    public void response(Message message)
    {
        responses.add(message); // we'll go ahead and resolve a reply from anyone, even if it's not from this dc

        int n;
        n = localdc.equals(snitch.getDatacenter(message.getFrom())) 
                ? localResponses.decrementAndGet()
                : localResponses.get();

        if (n == 0 && responseResolver.isDataPresent(responses))
        {
            condition.signal();
        }
    }
    
    @Override
    public int determineBlockFor(ConsistencyLevel consistency_level, String table)
	{
		DatacenterShardStrategy stategy = (DatacenterShardStrategy) StorageService.instance.getReplicationStrategy(table);
		return (stategy.getReplicationFactor(localdc) / 2) + 1;
	}
}