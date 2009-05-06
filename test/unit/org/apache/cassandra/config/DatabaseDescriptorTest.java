package org.apache.cassandra.config;
import static org.testng.Assert.assertNotNull;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class DatabaseDescriptorTest
{
    @Test
    public void testShouldHaveConfigFileNameAvailable()
    {
        assertNotNull(DatabaseDescriptor.getConfigFileName(), "DatabaseDescriptor should always be able to return the file name of the config file");
    }
}
