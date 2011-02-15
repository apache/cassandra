package org.apache.cassandra.service;

import java.io.IOException;

import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.net.IAsyncCallback;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.utils.WrappedRunnable;

public class AsyncRepairCallback implements IAsyncCallback
{
    private final RowRepairResolver repairResolver;
    private final int count;

    public AsyncRepairCallback(RowRepairResolver repairResolver, int count)
    {
        this.repairResolver = repairResolver;
        this.count = count;
    }

    public void response(Message message)
    {
        repairResolver.preprocess(message);
        if (repairResolver.getMessageCount() == count)
        {
            StageManager.getStage(Stage.READ_REPAIR).execute(new WrappedRunnable()
            {
                protected void runMayThrow() throws DigestMismatchException, IOException
                {
                    repairResolver.resolve();
                }
            });
        }
    }

    public boolean isLatencyForSnitch()
    {
        return true;
    }
}
