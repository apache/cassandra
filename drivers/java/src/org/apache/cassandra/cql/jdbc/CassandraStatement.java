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

import static org.apache.cassandra.cql.jdbc.Utils.*;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.SQLNonTransientException;
import java.sql.SQLRecoverableException;
import java.sql.SQLSyntaxErrorException;
import java.sql.SQLTransientConnectionException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.regex.Pattern;

import org.apache.cassandra.thrift.CqlResult;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.SchemaDisagreementException;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.thrift.TException;

/**
 * Cassandra statement: implementation class for {@link PreparedStatement}.
 */

class CassandraStatement extends AbstractStatement implements Statement
{
    /**
     * The connection.
     */
    protected CassandraConnection connection;

    /**
     * The cql.
     */
    protected String cql;

    protected int fetchDirection = ResultSet.FETCH_FORWARD;

    protected int fetchSize = 0;

    protected int maxFieldSize = 0;

    protected int maxRows = 0;

    protected int resultSetType = CResultSet.DEFAULT_TYPE;

    protected int resultSetConcurrency = CResultSet.DEFAULT_CONCURRENCY;

    protected int resultSetHoldability = CResultSet.DEFAULT_HOLDABILITY;

    protected ResultSet currentResultSet = null;

    protected int updateCount = -1;

    protected boolean escapeProcessing = true;

    CassandraStatement(CassandraConnection con) throws SQLException
    {
        this(con, null);
    }

    CassandraStatement(CassandraConnection con, String cql) throws SQLException
    {
        this.connection = con;
        this.cql = cql;
    }

    CassandraStatement(CassandraConnection con, String cql, int resultSetType, int resultSetConcurrency) throws SQLException
    {
        this(con, cql, resultSetType, resultSetConcurrency, ResultSet.HOLD_CURSORS_OVER_COMMIT);
    }

    CassandraStatement(CassandraConnection con, String cql, int resultSetType, int resultSetConcurrency,
                       int resultSetHoldability) throws SQLException
    {
        this.connection = con;
        this.cql = cql;

        if (!(resultSetType == ResultSet.TYPE_FORWARD_ONLY
              || resultSetType == ResultSet.TYPE_SCROLL_INSENSITIVE
              || resultSetType == ResultSet.TYPE_SCROLL_SENSITIVE)) throw new SQLSyntaxErrorException(BAD_TYPE_RSET);
        this.resultSetType = resultSetType;

        if (!(resultSetConcurrency == ResultSet.CONCUR_READ_ONLY
              || resultSetConcurrency == ResultSet.CONCUR_UPDATABLE)) throw new SQLSyntaxErrorException(BAD_TYPE_RSET);
        this.resultSetConcurrency = resultSetConcurrency;


        if (!(resultSetHoldability == ResultSet.HOLD_CURSORS_OVER_COMMIT
              || resultSetHoldability == ResultSet.CLOSE_CURSORS_AT_COMMIT))
            throw new SQLSyntaxErrorException(BAD_HOLD_RSET);
        this.resultSetHoldability = resultSetHoldability;
    }

    public void addBatch(String arg0) throws SQLException
    {
        checkNotClosed();
        throw new SQLFeatureNotSupportedException(NO_BATCH);
    }

    private final void checkNotClosed() throws SQLException
    {
        if (isClosed()) throw new SQLRecoverableException(WAS_CLOSED_STMT);
    }

    public void clearBatch() throws SQLException
    {
        checkNotClosed();
        throw new SQLFeatureNotSupportedException(NO_BATCH);
    }

    public void clearWarnings() throws SQLException
    {
        // This implementation does not support the collection of warnings so clearing is a no-op
        // but it is still an exception to call this on a closed connection.
        checkNotClosed();
    }

    public void close() throws SQLException
    {
        connection = null;
        cql = null;
    }

    private void doExecute(String sql) throws SQLException
    {
        try
        {
            resetResults();
            CqlResult rSet = connection.execute(sql);
            String keyspace = connection.currentKeyspace;

            switch (rSet.getType())
            {
                case ROWS:
                    currentResultSet = new CResultSet(this, rSet, keyspace);
                    break;
                case INT:
                    updateCount = rSet.getNum();
                    break;
                case VOID:
                    updateCount = 0;
                    break;
            }
        }
        catch (InvalidRequestException e)
        {
            throw new SQLSyntaxErrorException(e.getWhy());
        }
        catch (UnavailableException e)
        {
            throw new SQLNonTransientConnectionException(NO_SERVER, e);
        }
        catch (TimedOutException e)
        {
            throw new SQLTransientConnectionException(e.getMessage());
        }
        catch (SchemaDisagreementException e)
        {
            throw new SQLRecoverableException(SCHEMA_MISMATCH);
        }
        catch (TException e)
        {
            throw new SQLNonTransientConnectionException(e.getMessage());
        }

    }

    public boolean execute(String query) throws SQLException
    {
        checkNotClosed();
        doExecute(query);
        return !(currentResultSet == null);
    }

    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException
    {
        checkNotClosed();

        if (!(autoGeneratedKeys == RETURN_GENERATED_KEYS || autoGeneratedKeys == NO_GENERATED_KEYS))
            throw new SQLSyntaxErrorException(BAD_AUTO_GEN);

        if (autoGeneratedKeys == RETURN_GENERATED_KEYS) throw new SQLFeatureNotSupportedException(NO_GEN_KEYS);

        return execute(sql);
    }

    public int[] executeBatch() throws SQLException
    {
        throw new SQLFeatureNotSupportedException(NO_BATCH);
    }

    public ResultSet executeQuery(String query) throws SQLException
    {
        checkNotClosed();
        doExecute(query);
        if (currentResultSet == null)
            throw new SQLNonTransientException(NO_RESULTSET);
        return currentResultSet;
    }

    public int executeUpdate(String query) throws SQLException
    {
        checkNotClosed();
        doExecute(query);
        if (currentResultSet != null)
            throw new SQLNonTransientException(NO_UPDATE_COUNT);
        return updateCount;
    }

    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException
    {
        checkNotClosed();

        if (!(autoGeneratedKeys == RETURN_GENERATED_KEYS || autoGeneratedKeys == NO_GENERATED_KEYS))
            throw new SQLFeatureNotSupportedException(BAD_AUTO_GEN);

        return executeUpdate(sql);
    }

    public Connection getConnection() throws SQLException
    {
        checkNotClosed();
        return (Connection) connection;
    }

    public int getFetchDirection() throws SQLException
    {
        checkNotClosed();
        return fetchDirection;
    }

    public int getFetchSize() throws SQLException
    {
        checkNotClosed();
        return fetchSize;
    }

    public int getMaxFieldSize() throws SQLException
    {
        checkNotClosed();
        return maxFieldSize;
    }

    public int getMaxRows() throws SQLException
    {
        checkNotClosed();
        return maxRows;
    }

    public boolean getMoreResults() throws SQLException
    {
        checkNotClosed();
        resetResults();
        // in the current Cassandra implementation there are never MORE results
        return false;
    }

    public boolean getMoreResults(int current) throws SQLException
    {
        checkNotClosed();

        switch (current)
        {
            case CLOSE_CURRENT_RESULT:
                resetResults();
                break;

            case CLOSE_ALL_RESULTS:
            case KEEP_CURRENT_RESULT:
                throw new SQLFeatureNotSupportedException(NO_MULTIPLE);

            default:
                throw new SQLSyntaxErrorException(String.format(BAD_KEEP_RSET, current));
        }
        // in the current Cassandra implementation there are never MORE results
        return false;
    }

    public int getQueryTimeout() throws SQLException
    {
        // the Cassandra implementation does not support timeouts on queries
        return 0;
    }

    public ResultSet getResultSet() throws SQLException
    {
        checkNotClosed();
        return currentResultSet;
    }

    public int getResultSetConcurrency() throws SQLException
    {
        checkNotClosed();
        return ResultSet.CONCUR_READ_ONLY;
    }

    public int getResultSetHoldability() throws SQLException
    {
        checkNotClosed();
        // the Cassandra implementations does not support commits so this is the closest match
        return ResultSet.HOLD_CURSORS_OVER_COMMIT;
    }

    public int getResultSetType() throws SQLException
    {
        checkNotClosed();
        return ResultSet.TYPE_FORWARD_ONLY;
    }

    public int getUpdateCount() throws SQLException
    {
        checkNotClosed();
        return updateCount;
    }

    public SQLWarning getWarnings() throws SQLException
    {
        checkNotClosed();
        return null;
    }

    public boolean isClosed() throws SQLException
    {
        return connection == null;
    }

    public boolean isPoolable() throws SQLException
    {
        checkNotClosed();
        return false;
    }

    public boolean isWrapperFor(Class<?> iface) throws SQLException
    {
        return false;
    }

    private final void resetResults()
    {
        currentResultSet = null;
        updateCount = -1;
    }

    public void setEscapeProcessing(boolean enable) throws SQLException
    {
        checkNotClosed();
        // the Cassandra implementation does not currently look at this
        escapeProcessing = enable;
    }

    public void setFetchDirection(int direction) throws SQLException
    {
        checkNotClosed();

        if (direction == ResultSet.FETCH_FORWARD || direction == ResultSet.FETCH_REVERSE || direction == ResultSet.FETCH_UNKNOWN)
        {
            if ((getResultSetType() == ResultSet.TYPE_FORWARD_ONLY) && (direction != ResultSet.FETCH_FORWARD))
                throw new SQLSyntaxErrorException(String.format(BAD_FETCH_DIR, direction));
            fetchDirection = direction;
        }
        else throw new SQLSyntaxErrorException(String.format(BAD_FETCH_DIR, direction));
    }


    public void setFetchSize(int size) throws SQLException
    {
        checkNotClosed();
        if (size < 0) throw new SQLSyntaxErrorException(String.format(BAD_FETCH_SIZE, size));
        fetchSize = size;
    }

    public void setMaxFieldSize(int arg0) throws SQLException
    {
        checkNotClosed();
        // silently ignore this setting. always use default 0 (unlimited)
    }

    public void setMaxRows(int arg0) throws SQLException
    {
        checkNotClosed();
        // silently ignore this setting. always use default 0 (unlimited)
    }

    public void setPoolable(boolean poolable) throws SQLException
    {
        checkNotClosed();
        // silently ignore any attempt to set this away from the current default (false)
    }

    public void setQueryTimeout(int arg0) throws SQLException
    {
        checkNotClosed();
        // silently ignore any attempt to set this away from the current default (0)
    }

    public <T> T unwrap(Class<T> iface) throws SQLException
    {
        throw new SQLFeatureNotSupportedException(String.format(NO_INTERFACE, iface.getSimpleName()));
    }
}
