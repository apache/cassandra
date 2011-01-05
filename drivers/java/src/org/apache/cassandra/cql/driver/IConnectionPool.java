package org.apache.cassandra.cql.driver;

public interface IConnectionPool
{
    /**
     * Check a <code>Connection</code> instance out from the pool, creating a
     * new one if the pool is exhausted.
     */
    public Connection borrowConnection();
    
    /**
     * Returns an <code>Connection</code> instance to the pool.  If the pool
     * already contains the maximum number of allowed connections, then the
     * instance's <code>close</code> method is called and it is discarded.
     */
    public void returnConnection(Connection connection);
}
