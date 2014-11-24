package org.apache.cassandra.repair;

/**
 * Specify the degree of parallelism when calculating the merkle trees in a repair job.
 */
public enum RepairParallelism
{
    /**
     * One node at a time
     */
    SEQUENTIAL,

    /**
     * All nodes at the same time
     */
    PARALLEL,

    /**
     * One node per data center at a time
     */
    DATACENTER_AWARE
}
