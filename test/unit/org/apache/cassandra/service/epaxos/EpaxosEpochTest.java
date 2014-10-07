package org.apache.cassandra.service.epaxos;

import org.junit.Test;

public class EpaxosEpochTest
{
    /**
     * When an instance is the last/only instance executed against a partition,
     * it should be GC'd after 2 epochs pass.
     * @throws Exception
     */
    @Test
    public void partitionGC() throws Exception
    {

    }

    /**
     * When a token's epoch is incremented, the epochs for all
     * of it's key managers should be incremented as well
     */
    @Test
    public void keyManagerEpochIsIncremented() throws Exception
    {

    }
}
