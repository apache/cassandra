package org.apache.cassandra;

import org.testng.annotations.Test;
import org.testng.annotations.BeforeMethod;

import java.io.File;

@Test(groups={"serial"})
public class ServerTest {
    // TODO clean up static structures too (e.g. memtables)
    @BeforeMethod
    public void cleanup() {
        // for convenience, this assumes that you haven't changed the test config away from storing everything
        // under /var/cassandra.
        for (String dirname : new String[] {"bootstrap", "commitlog", "data", "staging", "system"}) {
            File dir = new File("/var/cassandra", dirname);
            for (File f : dir.listFiles()) {
                f.delete();
            }
        }
    }
}
