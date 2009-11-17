/**
 *
 */
package org.apache.cassandra.service;

import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.net.Message;

/**
 * This class will block for the replication factor which is
 * provided in the input map. it will block till we recive response from
 * n nodes in each of our data centers.
 */
public class DatacenterSyncWriteResponseHandler extends WriteResponseHandler
{
    private final Map<String, Integer> dcResponses = new HashMap<String, Integer>();
    private final Map<String, Integer> responseCounts;

    public DatacenterSyncWriteResponseHandler(Map<String, Integer> responseCounts)
    {
        // Response is been managed by the map so make it 1 for the superclass.
        super(1);
        this.responseCounts = responseCounts;
    }

    @Override
    public void response(Message message)
    {
        if (condition.isSignaled())
        {
            return;
        }
        try
        {
            String dataCenter = DatabaseDescriptor.getEndPointSnitch().getLocation(message.getFrom());
            Object blockFor = responseCounts.get(dataCenter);
            // If this DC needs to be blocked then do the below.
            if (blockFor != null)
            {
                Integer quorumCount = dcResponses.get(dataCenter);
                if (quorumCount == null)
                {
                    // Intialize and recognize the first response
                    dcResponses.put(dataCenter, 1);
                }
                else if ((Integer) blockFor > quorumCount)
                {
                    // recognize the consequtive responses.
                    dcResponses.put(dataCenter, quorumCount + 1);
                }
                else
                {
                    // No need to wait on it anymore so remove it.
                    responseCounts.remove(dataCenter);
                }
            }
        }
        catch (UnknownHostException e)
        {
            throw new RuntimeException(e);
        }
        responses.add(message);
        // If done then the response count will be empty after removing
        // everything.
        if (responseCounts.isEmpty())
        {
            condition.signal();
        }
    }
}
