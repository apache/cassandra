package org.apache.cassandra;

import org.testng.annotations.Test;
import org.testng.annotations.BeforeMethod;
import org.apache.cassandra.config.DatabaseDescriptor;

import java.io.File;

@Test(groups={"serial"})
public class ServerTest {
    // TODO clean up static structures too (e.g. memtables)
    @BeforeMethod
    public void cleanup()
    {
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
        }
    }
