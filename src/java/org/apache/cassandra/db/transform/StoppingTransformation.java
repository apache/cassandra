package org.apache.cassandra.db.transform;

import net.nicoulaj.compilecommand.annotations.DontInline;
import org.apache.cassandra.db.rows.BaseRowIterator;

// A Transformation that can stop an iterator earlier than its natural exhaustion
public abstract class StoppingTransformation<I extends BaseRowIterator<?>> extends Transformation<I>
{
    private BaseIterator.Stop stop;
    private BaseIterator.Stop stopInPartition;

    /**
     * If invoked by a subclass, any partitions iterator this transformation has been applied to will terminate
     * after any currently-processing item is returned, as will any row/unfiltered iterator
     */
    @DontInline
    protected void stop()
    {
        if (stop != null)
            stop.isSignalled = true;
        stopInPartition();
    }

    /**
     * If invoked by a subclass, any rows/unfiltered iterator this transformation has been applied to will terminate
     * after any currently-processing item is returned
     */
    @DontInline
    protected void stopInPartition()
    {
        if (stopInPartition != null)
            stopInPartition.isSignalled = true;
    }

    @Override
    protected void attachTo(BasePartitions partitions)
    {
        assert this.stop == null;
        this.stop = partitions.stop;
    }

    @Override
    protected void attachTo(BaseRows rows)
    {
        assert this.stopInPartition == null;
        this.stopInPartition = rows.stop;
    }

    @Override
    protected void onClose()
    {
        stop = null;
    }

    @Override
    protected void onPartitionClose()
    {
        stopInPartition = null;
    }
}
