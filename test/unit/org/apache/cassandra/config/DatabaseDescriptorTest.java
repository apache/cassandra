package org.apache.cassandra.config;

import static org.junit.Assert.assertNotNull;
import org.junit.Test;

public class DatabaseDescriptorTest
{
    @Test
    public void testShouldHaveConfigFileNameAvailable()
    {
        assertNotNull(DatabaseDescriptor.getConfigFileName(), "DatabaseDescriptor should always be able to return the file name of the config file");
    }
}
