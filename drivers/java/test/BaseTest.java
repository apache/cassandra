
import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;

import junit.framework.TestCase;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.service.EmbeddedCassandraService;

/**
 * The abstract BaseClass.
 */
public abstract class BaseTest extends TestCase
{

    /** The embedded server cassandra. */
    private static EmbeddedCassandraService cassandra;
    
    /**
     * Start cassandra server.
     * @throws ConfigurationException 
     *
     * @throws Exception the exception
     */
    protected void startCassandraServer() throws IOException, ConfigurationException
    {
        if (!checkIfServerRunning())
        {
            System.setProperty("cassandra.config", "cassandra.yaml");
            loadData();
            cassandra = new EmbeddedCassandraService();
            cassandra.start();
        }
    }

    
    /**
     * Load yaml tables.
     *
     * @throws ConfigurationException the configuration exception
     */
    private void loadData() throws ConfigurationException
    {
        for (KSMetaData table : DatabaseDescriptor.readTablesFromYaml())
        {
            for (CFMetaData cfm : table.cfMetaData().values())
            {
                CFMetaData.map(cfm);
            }
            DatabaseDescriptor.setTableDefinition(table, DatabaseDescriptor.getDefsVersion());
        }
    }
    /**
     * Check if server running.
     *
     * @return true, if successful
     */
    private boolean checkIfServerRunning()
    {
        try
        {
            Socket socket = new Socket("127.0.0.1", 9170);
            return socket.getInetAddress() != null;
        } 
        catch (UnknownHostException e)
        {
            return false;
        }
        catch (IOException e)
        {
            return false;
        }
    }
}