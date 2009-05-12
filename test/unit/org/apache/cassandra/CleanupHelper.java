package org.apache.cassandra;

import java.io.File;

import org.junit.BeforeClass;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.log4j.Logger;

public class CleanupHelper
{
    private static Logger logger = Logger.getLogger(CleanupHelper.class);

    @BeforeClass
    public static void cleanup()
    {
        // we clean the fs twice, once to start with (so old data files don't get stored by anything static if this is the first run)
        // and once after flushing stuff (to try to clean things out if it is not.)  part #2 seems to be less than perfect.
        String[] directoryNames = {
                DatabaseDescriptor.getBootstrapFileLocation(),
                DatabaseDescriptor.getLogFileLocation(),
                DatabaseDescriptor.getDataFileLocation(),
                DatabaseDescriptor.getMetadataDirectory(),
        };

        for (String dirName : directoryNames)
        {
            File dir = new File(dirName);
            if (!dir.exists())
            {
                throw new RuntimeException("No such directory: " + dir.getAbsolutePath());
            }
            for (File f : dir.listFiles())
            {
                logger.debug("deleting " + f);
                f.delete();
            }
        }
    }
}