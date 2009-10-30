/**
 *
 */
package org.apache.cassandra.service;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.locator.IEndPointSnitch;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.utils.FBUtilities;

/**
 * This class will basically will block for the replication factor which is
 * provided in the input map. it will block till we recive response from (DC, n)
 * nodes.
 */
public class DatacenterQuorumResponseHandler<T> extends QuorumResponseHandler<T>
{
    private int blockFor;
    private IEndPointSnitch endpointsnitch;
    private InetAddress localEndpoint;

    public DatacenterQuorumResponseHandler(int blockFor, IResponseResolver<T> responseResolver)
    throws InvalidRequestException
    {
        // Response is been managed by the map so the waitlist size really doesnt matter.
        super(blockFor, responseResolver);
        this.blockFor = blockFor;
        endpointsnitch = DatabaseDescriptor.getEndPointSnitch();
        localEndpoint = FBUtilities.getLocalAddress();
    }

    @Override
    public void response(Message message)
    {
        // IF done look no futher.
        if (condition_.isSignaled())
        {
            return;
        }
            //Is optimal to check if same datacenter than comparing Arrays.
        try
        {
            if (endpointsnitch.isInSameDataCenter(localEndpoint, message.getFrom()))
            {
                blockFor--;
            }
        }
        catch (UnknownHostException e)
        {
            throw new RuntimeException(e);
        }
        responses_.add(message);
        if (blockFor <= 0)
        {
            //Singnal when Quorum is recived.
            condition_.signal();
        }
        if (logger_.isDebugEnabled())
            logger_.debug("Processed Message: " + message.toString());
    }
}
