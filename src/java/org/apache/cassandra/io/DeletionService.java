package org.apache.cassandra.io;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ExecutorService;

import org.apache.cassandra.concurrent.JMXEnabledThreadPoolExecutor;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.WrappedRunnable;

public class DeletionService
{
    public static final ExecutorService executor = new JMXEnabledThreadPoolExecutor("FILEUTILS-DELETE-POOL");

    public static void deleteAsync(final String file) throws IOException
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
}
