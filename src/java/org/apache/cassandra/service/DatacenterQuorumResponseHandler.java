package org.apache.cassandra.service;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.locator.DatacenterShardStrategy;
import org.apache.cassandra.locator.RackInferringSnitch;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.utils.FBUtilities;

/**
 * Datacenter Quorum response handler will make sure to that all the responses
 * recived are from the same dc and will validate it with the
 * 
 * @author Vijay Parthasarathy
 */
public class DatacenterQuorumResponseHandler<T> extends QuorumResponseHandler<T>
{
    private static final RackInferringSnitch snitch = (RackInferringSnitch) DatabaseDescriptor.getEndpointSnitch();
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
        try
        {
            int b = -1;
            responses.add(message);
            // If DCQuorum/DCQuorumSync, check if the response is from the local DC.
            if (localdc.equals(snitch.getDatacenter(message.getFrom())))
            {
                b = localResponses.decrementAndGet();
            } else {
            	b = localResponses.get();
            }
            if (b == 0 && responseResolver.isDataPresent(responses))
            {
                condition.signal();
            }
        }
        catch (Exception ex)
        {
            throw new RuntimeException(ex);
        }
    }
    
    @Override
    public int determineBlockFor(ConsistencyLevel consistency_level, String table)
	{
		DatacenterShardStrategy stategy = (DatacenterShardStrategy) StorageService.instance.getReplicationStrategy(table);
		return (stategy.getReplicationFactor(localdc, table)/2) + 1;
	}
}