package org.apache.cassandra.io;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ExecutorService;

import org.apache.cassandra.concurrent.JMXEnabledThreadPoolExecutor;
import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.WrappedRunnable;

public class DeletionService
{
    public static final int MAX_RETRIES = 10;

    public static final ExecutorService executor = new JMXEnabledThreadPoolExecutor("FILEUTILS-DELETE-POOL");

    public static void submitDelete(final String file)
    {
        Runnable deleter = new WrappedRunnable()
        {
            @Override
            protected void runMayThrow() throws IOException
            {
                FileUtils.deleteWithConfirm(new File(file));
            }
        };
        executor.submit(deleter);
    }

    public static void submitDeleteWithRetry(String file)
    {
        submitDeleteWithRetry(file, 0);
    }

    private static void submitDeleteWithRetry(final String file, final int retryCount)
    {
        Runnable deleter = new WrappedRunnable()
        {
            @Override
            protected void runMayThrow() throws IOException
            {
                if (!new File(file).delete())
                {
                    if (retryCount > MAX_RETRIES)
                        throw new IOException("Unable to delete " + file + " after " + MAX_RETRIES + " tries");
                    new Thread(new Runnable()
                    {
                        public void run()
                        {
                            try
                            {
                                Thread.sleep(10000);
                            }
                            catch (InterruptedException e)
                            {
                                throw new AssertionError(e);
                            }
                            submitDeleteWithRetry(file, retryCount + 1);
                        }
                    }, "Delete submission: " + file).start();
                }
            }
        };
        executor.submit(deleter);
    }
}
