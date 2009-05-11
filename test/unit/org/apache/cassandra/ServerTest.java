package org.apache.cassandra;

import org.testng.annotations.Test;
import org.testng.annotations.BeforeMethod;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Table;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.CommitLog;

import java.io.File;
import java.io.IOException;

@Test(groups={"serial"})
public class ServerTest {
    @BeforeMethod
    public void cleanup()
    {
        // we clean the fs twice, once to start with (so old data files don't get stored by anything static if this is the first run)
        // and once after flushing stuff (to try to clean things out if it is not.)  part #2 seems to be less than perfect.
        cleanUpFilesystem();

        Table table = Table.open("Table1");
        for (String cfName : table.getColumnFamilies())
        {
            ColumnFamilyStore cfs = table.getColumnFamilyStore(cfName);
            try
            {
                cfs.reset();
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
        }

        CommitLog.reset();

        cleanUpFilesystem();
    }

    private void cleanUpFilesystem()
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
            for (File f : dir.listFiles())
            {
                f.delete();
            }
        }
    }
}