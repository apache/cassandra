package org.apache.cassandra.io;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ExecutorService;

import org.apache.cassandra.concurrent.JMXEnabledThreadPoolExecutor;
import org.apache.cassandra.io.util.FileUtils;

public class DeletionService
{
    public static final ExecutorService executor = new JMXEnabledThreadPoolExecutor("FILEUTILS-DELETE-POOL");

    public static void deleteAsync(final String file) throws IOException
    {
        Runnable deleter = new Runnable()
        {
            public void run()
            {
                try
                {
                    FileUtils.deleteWithConfirm(new File(file));
                }
                catch (IOException e)
                {
                    throw new RuntimeException(e);
                }
            }
        };
        executor.submit(deleter);
    }
}
