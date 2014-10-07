package org.apache.cassandra.service.epaxos;

import org.junit.Test;

/**
 * Tests the leader of a prepare phase
 */
public class EpaxosPrepareLeaderTest
{
    @Test
    public void quorumFailure() throws Exception
    {

    }

    @Test
    public void ballotFailure() throws Exception
    {

    }

    /**
     * Tests that prepare returns immediately if the
     * requested instance is already committed
     */
    @Test
    public void committedInstanceNoop() throws Exception
    {

    }

    /**
     * Tests that waiting prepare phase will abort if another
     * thread commits it first
     */
    @Test
    public void commitNotification() throws Exception
    {

    }

    /**
     * Once a quorum of responses is received, additional responses should be discarded
     */
    @Test
    public void lateResponseIsDiscarded() throws Exception
    {

    }
}
