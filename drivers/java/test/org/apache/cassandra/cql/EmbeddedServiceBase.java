package org.apache.cassandra.cql;

import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.service.EmbeddedCassandraService;

/**
 * The abstract BaseClass.
 */
public abstract class EmbeddedServiceBase
{

    /** The embedded server cassandra. */
    private static EmbeddedCassandraService cassandra;
    
    /**
     * Start cassandra server.
     * @throws ConfigurationException 
     *
     * @throws Exception the exception
     */
    static void startCassandraServer() throws IOException, ConfigurationException
    {
        if (!checkIfServerRunning())
        {
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
    static void loadData() throws ConfigurationException
    {
        for (KSMetaData table : SchemaLoader.schemaDefinition())
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
    static boolean checkIfServerRunning()
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