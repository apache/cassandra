
package org.apache.cassandra.cql.jdbc;

import static org.apache.cassandra.cql.jdbc.Utils.HOST_REQUIRED;
import static org.apache.cassandra.cql.jdbc.Utils.NO_INTERFACE;
import static org.apache.cassandra.cql.jdbc.Utils.PROTOCOL;
import static org.apache.cassandra.cql.jdbc.Utils.TAG_SERVER_NAME;
import static org.apache.cassandra.cql.jdbc.Utils.TAG_DATABASE_NAME;
import static org.apache.cassandra.cql.jdbc.Utils.TAG_PASSWORD;
import static org.apache.cassandra.cql.jdbc.Utils.TAG_PORT_NUMBER;
import static org.apache.cassandra.cql.jdbc.Utils.TAG_USER;
import static org.apache.cassandra.cql.jdbc.Utils.createSubName;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLNonTransientConnectionException;
import java.util.Properties;

import javax.sql.DataSource;

public class CassandraDataSource implements DataSource
{

    static
    {
        try
        {
            Class.forName("org.apache.cassandra.cql.jdbc.CassandraDriver");
        }
        catch (ClassNotFoundException e)
        {
            throw new RuntimeException(e);
        }
    }

    protected static final String description = "Cassandra Data Source";

    protected String serverName;

    protected int    portNumber = 9160;

    protected String databaseName;

    protected String user;

    protected String password;

    public CassandraDataSource(String host, int port, String keyspace, String user, String password)
    {
        if (host != null) setServerName(host);
        if (port != -1) setPortNumber(port);
        setDatabaseName(keyspace);
        setUser(user);
        setPassword(password);
    }

    public String getDescription()
    {
        return description;
    }

    public String getServerName()
    {
        return serverName;
    }

    public void setServerName(String serverName)
    {
        this.serverName = serverName;
    }

    public int getPortNumber()
    {
        return portNumber;
    }

    public void setPortNumber(int portNumber)
    {
        this.portNumber = portNumber;
    }

    public String getDatabaseName()
    {
        return databaseName;
    }

    public void setDatabaseName(String databaseName)
    {
        this.databaseName = databaseName;
    }

    public String getUser()
    {
        return user;
    }

    public void setUser(String user)
    {
        this.user = user;
    }

    public String getPassword()
    {
        return password;
    }

    public void setPassword(String password)
    {
        this.password = password;
    }

    public Connection getConnection() throws SQLException
    {
        return getConnection(null, null);
    }

    public Connection getConnection(String user, String password) throws SQLException
    {
        Properties props = new Properties();
        
        this.user = user;
        this.password = password;
        
        if (this.serverName!=null) props.setProperty(TAG_SERVER_NAME, this.serverName);
        else throw new SQLNonTransientConnectionException(HOST_REQUIRED);
        props.setProperty(TAG_PORT_NUMBER, ""+this.portNumber);
        if (this.databaseName!=null) props.setProperty(TAG_DATABASE_NAME, this.databaseName);
        if (user!=null) props.setProperty(TAG_USER, user);
        if (password!=null) props.setProperty(TAG_PASSWORD, password);

        String url = PROTOCOL+createSubName(props);
        return DriverManager.getConnection(url, props);
    }

    public int getLoginTimeout() throws SQLException
    {
        return DriverManager.getLoginTimeout();
    }

    public PrintWriter getLogWriter() throws SQLException
    {
        return DriverManager.getLogWriter();
    }

    public void setLoginTimeout(int timeout) throws SQLException
    {
        DriverManager.setLoginTimeout(timeout);
    }

    public void setLogWriter(PrintWriter writer) throws SQLException
    {
        DriverManager.setLogWriter(writer);
    }

    public boolean isWrapperFor(Class<?> iface) throws SQLException
    {
        return iface.isAssignableFrom(getClass());
    }

    public <T> T unwrap(Class<T> iface) throws SQLException
    {
        if (iface.isAssignableFrom(getClass())) return iface.cast(this);
        throw new SQLFeatureNotSupportedException(String.format(NO_INTERFACE, iface.getSimpleName()));
    }      
}
