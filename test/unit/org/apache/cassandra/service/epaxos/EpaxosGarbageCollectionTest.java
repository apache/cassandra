package org.apache.cassandra.service.epaxos;

import org.junit.Test;

public class EpaxosGarbageCollectionTest
{
    /**
     * Tests that the normal operation of the gc task
     * works as expected
     */
    @Test
    public void normalOperation()
    {

    }

    /**
     * tests that instance gc is skipped if token
     * state data/instances are being streamed to a peer
     */
    @Test
    public void skippedWhileStreamingInstances()
    {

    }
}
