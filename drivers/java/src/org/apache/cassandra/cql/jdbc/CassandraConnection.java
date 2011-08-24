/*
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * 
 */
package org.apache.cassandra.cql.jdbc;

import static org.apache.cassandra.cql.jdbc.Utils.ALWAYS_AUTOCOMMIT;
import static org.apache.cassandra.cql.jdbc.Utils.BAD_TIMEOUT;
import static org.apache.cassandra.cql.jdbc.Utils.NO_INTERFACE;
import static org.apache.cassandra.cql.jdbc.Utils.NO_TRANSACTIONS;
import static org.apache.cassandra.cql.jdbc.Utils.PROTOCOL;
import static org.apache.cassandra.cql.jdbc.Utils.SCHEMA_MISMATCH;
import static org.apache.cassandra.cql.jdbc.Utils.TAG_SERVER_NAME;
import static org.apache.cassandra.cql.jdbc.Utils.TAG_DATABASE_NAME;
import static org.apache.cassandra.cql.jdbc.Utils.TAG_PASSWORD;
import static org.apache.cassandra.cql.jdbc.Utils.TAG_PORT_NUMBER;
import static org.apache.cassandra.cql.jdbc.Utils.TAG_USER;
import static org.apache.cassandra.cql.jdbc.Utils.WAS_CLOSED_CON;
import static org.apache.cassandra.cql.jdbc.Utils.createSubName;
import static org.apache.cassandra.cql.jdbc.Utils.determineCurrentKeyspace;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLInvalidAuthorizationSpecException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.SQLRecoverableException;
import java.sql.SQLSyntaxErrorException;
import java.sql.SQLTimeoutException;
import java.sql.SQLTransientConnectionException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.cassandra.thrift.AuthenticationException;
import org.apache.cassandra.thrift.AuthenticationRequest;
import org.apache.cassandra.thrift.AuthorizationException;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Compression;
import org.apache.cassandra.thrift.CqlResult;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.SchemaDisagreementException;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation class for {@link Connection}.
 */
class CassandraConnection extends AbstractCassandraConnection implements Connection
{

    private static final Logger logger = LoggerFactory.getLogger(CassandraConnection.class);

    public static final int DB_MAJOR_VERSION = 0;
    public static final int DB_MINOR_VERSION = 8;
    public static final String DB_PRODUCT_NAME = "Cassandra";

    public static Compression defaultCompression = Compression.GZIP;

    private final boolean autoCommit = true;

    private final int transactionIsolation = Connection.TRANSACTION_NONE;

    /**
     * Client Info Properties (currently unused)
     */
    private Properties clientInfo = new Properties();

    /**
     * List of all Statements that have been created by this connection
     */
    private List<Statement> statements;

    private Cassandra.Client client;
    private TTransport transport;

    protected long timeOfLastFailure = 0;
    protected int numFailures = 0;
    protected String username = null;
    protected String url = null;
    String currentKeyspace;
    ColumnDecoder decoder;


    /**
     * Instantiates a new CassandraConnection.
     */
    public CassandraConnection(Properties props) throws SQLException
    {
        statements = new ArrayList<Statement>();
        clientInfo = new Properties();
        url = PROTOCOL + createSubName(props);
        try
        {
            String host = props.getProperty(TAG_SERVER_NAME);
            int port = Integer.parseInt(props.getProperty(TAG_PORT_NUMBER));
            String keyspace = props.getProperty(TAG_DATABASE_NAME);
            username = props.getProperty(TAG_USER);
            String password = props.getProperty(TAG_PASSWORD);

            TSocket socket = new TSocket(host, port);
            transport = new TFramedTransport(socket);
            TProtocol protocol = new TBinaryProtocol(transport);
            client = new Cassandra.Client(protocol);
            socket.open();
            decoder = new ColumnDecoder(client.describe_keyspaces());

            if (username != null)
            {
                Map<String, String> credentials = new HashMap<String, String>();
                credentials.put("username", username);
                if (password != null) credentials.put("password", password);
                AuthenticationRequest areq = new AuthenticationRequest(credentials);
                client.login(areq);

            }

            logger.info("Connected to {}:{}", host, port);


            if (keyspace != null)
            {
                execute("USE " + keyspace);
            }
        }
        catch (SchemaDisagreementException e)
        {
            throw new SQLRecoverableException(SCHEMA_MISMATCH);
        }
        catch (InvalidRequestException e)
        {
            throw new SQLSyntaxErrorException(e);
        }
        catch (UnavailableException e)
        {
            throw new SQLNonTransientConnectionException(e);
        }
        catch (TimedOutException e)
        {
            throw new SQLTransientConnectionException(e);
        }
        catch (TException e)
        {
            throw new SQLNonTransientConnectionException(e);
        }
        catch (AuthenticationException e)
        {
            throw new SQLInvalidAuthorizationSpecException(e);
        }
        catch (AuthorizationException e)
        {
            throw new SQLInvalidAuthorizationSpecException(e);
        }
    }

    private final void checkNotClosed() throws SQLException
    {
        if (isClosed()) throw new SQLNonTransientConnectionException(WAS_CLOSED_CON);
    }

    public void clearWarnings() throws SQLException
    {
        // This implementation does not support the collection of warnings so clearing is a no-op
        // but it is still an exception to call this on a closed connection.
        checkNotClosed();
    }

    /**
     * On close of connection.
     */
    public synchronized void close() throws SQLException
    {
        if (isConnected())
        {
            // spec says to close all statements associated with this connection upon close
            for (Statement statement : statements) statement.close();
            // then disconnect from the transport                
            disconnect();
        }
    }

    public void commit() throws SQLException
    {
        checkNotClosed();
        throw new SQLFeatureNotSupportedException(ALWAYS_AUTOCOMMIT);
    }

    public Statement createStatement() throws SQLException
    {
        checkNotClosed();
        statements.add(new CassandraStatement(this));
        return statements.get(statements.size() - 1);
    }

    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException
    {
        checkNotClosed();
        statements.add(new CassandraStatement(this, null, resultSetType, resultSetConcurrency));
        return statements.get(statements.size() - 1);
    }

    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException
    {
        checkNotClosed();
        statements.add(new CassandraStatement(this, null, resultSetType, resultSetConcurrency, resultSetHoldability));
        return statements.get(statements.size() - 1);
    }

    public boolean getAutoCommit() throws SQLException
    {
        checkNotClosed();
        return autoCommit;
    }

    public String getCatalog() throws SQLException
    {
        // This implementation does not support the catalog names so null is always returned if the connection is open.
        // but it is still an exception to call this on a closed connection.
        checkNotClosed();
        return null;
    }

    public Properties getClientInfo() throws SQLException
    {
        checkNotClosed();
        return clientInfo;
    }

    public String getClientInfo(String label) throws SQLException
    {
        checkNotClosed();
        return clientInfo.getProperty(label);
    }

    public int getHoldability() throws SQLException
    {
        checkNotClosed();
        // the rationale is there are really no commits in Cassandra so no boundary...
        return CResultSet.DEFAULT_HOLDABILITY;
    }

    public DatabaseMetaData getMetaData() throws SQLException
    {
        checkNotClosed();
        return new CassandraDatabaseMetaData(this);
    }

    public int getTransactionIsolation() throws SQLException
    {
        checkNotClosed();
        return transactionIsolation;
    }

    public SQLWarning getWarnings() throws SQLException
    {
        checkNotClosed();
        // the rationale is there are no warnings to return in this implementation...
        return null;
    }

    public synchronized boolean isClosed() throws SQLException
    {

        return !isConnected();
    }

    public boolean isReadOnly() throws SQLException
    {
        checkNotClosed();
        return false;
    }

    public boolean isValid(int timeout) throws SQLException
    {
        checkNotClosed();
        if (timeout < 0) throw new SQLTimeoutException(BAD_TIMEOUT);

        // this needs to be more robust. Some query needs to be made to verify connection is really up.
        return !isClosed();
    }

    public boolean isWrapperFor(Class<?> arg0) throws SQLException
    {
        return false;
    }

    public String nativeSQL(String sql) throws SQLException
    {
        checkNotClosed();
        // the rationale is there are no distinction between grammars in this implementation...
        // so we are just return the input argument
        return sql;
    }

    public PreparedStatement prepareStatement(String sql) throws SQLException
    {
        checkNotClosed();
        statements.add(new CassandraPreparedStatement(this, sql));
        return (PreparedStatement) statements.get(statements.size() - 1);
    }

    public PreparedStatement prepareStatement(String arg0, int arg1, int arg2) throws SQLException
    {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public PreparedStatement prepareStatement(String arg0, int arg1, int arg2, int arg3) throws SQLException
    {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public void rollback() throws SQLException
    {
        checkNotClosed();
        throw new SQLFeatureNotSupportedException(ALWAYS_AUTOCOMMIT);
    }

    public void setAutoCommit(boolean autoCommit) throws SQLException
    {
        checkNotClosed();
        if (!autoCommit) throw new SQLFeatureNotSupportedException(ALWAYS_AUTOCOMMIT);
    }

    public void setCatalog(String arg0) throws SQLException
    {
        checkNotClosed();
        // the rationale is there are no catalog name to set in this implementation...
        // so we are "silently ignoring" the request
    }

    public void setClientInfo(Properties props) throws SQLClientInfoException
    {
        // we don't use them but we will happily collect them for now...
        if (props != null) clientInfo = props;
    }

    public void setClientInfo(String key, String value) throws SQLClientInfoException
    {
        // we don't use them but we will happily collect them for now...
        clientInfo.setProperty(key, value);
    }

    public void setHoldability(int arg0) throws SQLException
    {
        checkNotClosed();
        // the rationale is there are no holdability to set in this implementation...
        // so we are "silently ignoring" the request
    }

    public void setReadOnly(boolean arg0) throws SQLException
    {
        checkNotClosed();
        // the rationale is all connections are read/write in the Cassandra implementation...
        // so we are "silently ignoring" the request
    }

    public void setTransactionIsolation(int level) throws SQLException
    {
        checkNotClosed();
        if (level != Connection.TRANSACTION_NONE) throw new SQLFeatureNotSupportedException(NO_TRANSACTIONS);
    }

    public <T> T unwrap(Class<T> iface) throws SQLException
    {
        throw new SQLFeatureNotSupportedException(String.format(NO_INTERFACE, iface.getSimpleName()));
    }

    /**
     * Execute a CQL query.
     *
     * @param queryStr    a CQL query string
     * @param compression query compression to use
     * @return the query results encoded as a CqlResult structure
     * @throws InvalidRequestException     on poorly constructed or illegal requests
     * @throws UnavailableException        when not all required replicas could be created/read
     * @throws TimedOutException           when a cluster operation timed out
     * @throws SchemaDisagreementException when the client side and server side are at different versions of schema (Thrift)
     * @throws TException                  when there is a error in Thrift processing
     */
    public CqlResult execute(String queryStr, Compression compression) throws InvalidRequestException, UnavailableException, TimedOutException, SchemaDisagreementException, TException
    {
        currentKeyspace = determineCurrentKeyspace(queryStr, currentKeyspace);

        try
        {
            return client.execute_cql_query(Utils.compressQuery(queryStr, compression), compression);
        }
        catch (TException error)
        {
            numFailures++;
            timeOfLastFailure = System.currentTimeMillis();
            throw error;
        }
    }

    /**
     * Execute a CQL query using the default compression methodology.
     *
     * @param queryStr a CQL query string
     * @return the query results encoded as a CqlResult structure
     * @throws InvalidRequestException     on poorly constructed or illegal requests
     * @throws UnavailableException        when not all required replicas could be created/read
     * @throws TimedOutException           when a cluster operation timed out
     * @throws SchemaDisagreementException when the client side and server side are at different versions of schema (Thrift)
     * @throws TException                  when there is a error in Thrift processing
     */
    public CqlResult execute(String queryStr) throws InvalidRequestException, UnavailableException, TimedOutException, SchemaDisagreementException, TException
    {
        return execute(queryStr, defaultCompression);
    }

    /**
     * Shutdown the remote connection
     */
    public void disconnect()
    {
        transport.close();
    }

    /**
     * Connection state.
     */
    public boolean isConnected()
    {
        return transport.isOpen();
    }
}
