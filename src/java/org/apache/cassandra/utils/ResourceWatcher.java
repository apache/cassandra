package org.apache.cassandra.utils;

import java.io.File;
import java.util.Timer;
import java.util.TimerTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ResourceWatcher
{
    private static Timer timer = new Timer("RESOURCE-WATCHER");

    public static void watch(String resource, Runnable callback, int period)
    {
        timer.schedule(new WatchedResource(resource, callback), period, period);
    }
    
    public static class WatchedResource extends TimerTask
    {
        private static Logger logger = LoggerFactory.getLogger(WatchedResource.class);
        private String resource;
        private Runnable callback;
        private long lastLoaded;

        public WatchedResource(String resource, Runnable callback)
        {
            this.resource = resource;
            this.callback = callback;
            lastLoaded = 0;
        }

        public void run()
        {
            try
            {
                String filename = FBUtilities.resourceToFile(resource);
                long lastModified = new File(filename).lastModified();
                if (lastModified > lastLoaded)
                {
                    callback.run();
                    lastLoaded = lastModified;
                }
            }
            catch (Throwable t)
            {
                logger.error(String.format("Timed run of %s failed.", callback.getClass()), t);
            }
        }
    }
}
