package org.apache.cassandra.triggers;

import java.nio.ByteBuffer;
import java.util.Collection;

import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.RowMutation;

/**
 * Trigger interface, For every Mutation received by the coordinator {@link #augment(ByteBuffer, ColumnFamily)}
 * is called.<p>
 *
 * <b> Contract:</b><br>
 * 1) Implementation of this interface should only have a constructor without parameters <br>
 * 2) ITrigger implementation can be instantiated multiple times during the server life time.
 *      (Depends on the number of times trigger folder is updated.)<br>
 * 3) ITrigger implementation should be state-less (avoid dependency on instance variables).<br>
 * 
 * <br><b>The API is still beta and can change.</b>
 */
public interface ITrigger
{
    /**
     * Called exactly once per CF update, returned mutations are atomically updated.
     *
     * @param key - Row Key for the update.
     * @param update - Update received for the CF
     * @return modifications to be applied, null if no action to be performed.
     */
    public Collection<RowMutation> augment(ByteBuffer key, ColumnFamily update);
}
