/**
 *
 */
package org.apache.cassandra.service;

import java.net.InetAddress;
import java.util.List;

import org.apache.cassandra.net.Message;

/**
 * This class will basically will block for the replication factor which is
 * provided in the input map. it will block till we recive response from (DC, n)
 * nodes.
 */
public class DatacenterQuorumResponseHandler<T> extends QuorumResponseHandler<T>
{
    private final List<InetAddress> waitList;
    private int blockFor;

    public DatacenterQuorumResponseHandler(List<InetAddress> waitList, int blockFor, IResponseResolver<T> responseResolver)
    throws InvalidRequestException
    {
        // Response is been managed by the map so the waitlist size really doesnt matter.
        super(blockFor, responseResolver);
        this.blockFor = blockFor;
        this.waitList = waitList;
    }

    @Override
    public void response(Message message)
    {
        if (condition_.isSignaled())
        {
            return;
        }

        if (waitList.contains(message.getFrom()))
        {
            blockFor--;
        }
        responses_.add(message);
        // If done then the response count will be empty after removing
        // everything.
        if (blockFor <= 0)
        {
            condition_.signal();
        }
    }
}
