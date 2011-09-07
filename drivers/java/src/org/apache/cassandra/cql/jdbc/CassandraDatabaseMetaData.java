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

import static org.apache.cassandra.cql.jdbc.Utils.NO_INTERFACE;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;

class CassandraDatabaseMetaData implements DatabaseMetaData
{
    private CassandraConnection connection;
    
    public CassandraDatabaseMetaData(CassandraConnection connection)
    {
        this.connection = connection;
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

    public boolean allProceduresAreCallable() throws SQLException
    {
        return false;
    }

    public boolean allTablesAreSelectable() throws SQLException
    {
        return true;
    }

    public boolean autoCommitFailureClosesAllResultSets() throws SQLException
    {
        return false;
    }

    public boolean dataDefinitionCausesTransactionCommit() throws SQLException
    {
        return false;
    }

    public boolean dataDefinitionIgnoredInTransactions() throws SQLException
    {
        return false;
    }

    public boolean deletesAreDetected(int arg0) throws SQLException
    {
        return false;
    }

    public boolean doesMaxRowSizeIncludeBlobs() throws SQLException
    {
        return false;
    }

    public ResultSet getAttributes(String arg0, String arg1, String arg2, String arg3) throws SQLException
    {
        return new CResultSet();
    }

    public ResultSet getBestRowIdentifier(String arg0, String arg1, String arg2, int arg3, boolean arg4) throws SQLException
    {
        return new CResultSet();
    }

    public String getCatalogSeparator() throws SQLException
    {
        return "";
    }

    public String getCatalogTerm() throws SQLException
    {
        return "";
    }

    public ResultSet getCatalogs() throws SQLException
    {
        return new CResultSet();
    }

    public ResultSet getClientInfoProperties() throws SQLException
    {
        return new CResultSet();
    }

    public ResultSet getColumnPrivileges(String arg0, String arg1, String arg2, String arg3) throws SQLException
    {
        return new CResultSet();
    }

    public ResultSet getColumns(String arg0, String arg1, String arg2, String arg3) throws SQLException
    {
        return new CResultSet();
    }

    public Connection getConnection() throws SQLException
    {
        return connection;
    }

    public ResultSet getCrossReference(String arg0, String arg1, String arg2, String arg3, String arg4, String arg5) throws SQLException
    {
        return new CResultSet();
    }

    public int getDatabaseMajorVersion() throws SQLException
    {
        return CassandraConnection.DB_MAJOR_VERSION;
    }

    public int getDatabaseMinorVersion() throws SQLException
    {
        return CassandraConnection.DB_MINOR_VERSION;
    }

    public String getDatabaseProductName() throws SQLException
    {
        return CassandraConnection.DB_PRODUCT_NAME;
    }

    public String getDatabaseProductVersion() throws SQLException
    {
        return String.format("%d.%d", CassandraConnection.DB_MAJOR_VERSION,CassandraConnection.DB_MINOR_VERSION);
    }

    public int getDefaultTransactionIsolation() throws SQLException
    {
        return Connection.TRANSACTION_NONE;
    }

    public int getDriverMajorVersion()
    {
        return CassandraDriver.DVR_MAJOR_VERSION;
    }

    public int getDriverMinorVersion()
    {
        return CassandraDriver.DVR_MINOR_VERSION;
    }

    public String getDriverName() throws SQLException
    {
        return CassandraDriver.DVR_NAME;
    }

    public String getDriverVersion() throws SQLException
    {
        return String.format("%d.%d.%d", CassandraDriver.DVR_MAJOR_VERSION,CassandraDriver.DVR_MINOR_VERSION,CassandraDriver.DVR_PATCH_VERSION);
    }

    public ResultSet getExportedKeys(String arg0, String arg1, String arg2) throws SQLException
    {
        return new CResultSet();
    }

    public String getExtraNameCharacters() throws SQLException
    {
        return "";
    }

    public ResultSet getFunctionColumns(String arg0, String arg1, String arg2, String arg3) throws SQLException
    {
        return new CResultSet();
    }

    public ResultSet getFunctions(String arg0, String arg1, String arg2) throws SQLException
    {
        return new CResultSet();
    }

    public String getIdentifierQuoteString() throws SQLException
    {
        return "'";
    }

    public ResultSet getImportedKeys(String arg0, String arg1, String arg2) throws SQLException
    {
        return new CResultSet();
    }

    public ResultSet getIndexInfo(String arg0, String arg1, String arg2, boolean arg3, boolean arg4) throws SQLException
    {
        return new CResultSet();
    }

    public int getJDBCMajorVersion() throws SQLException
    {
        return 4;
    }

    public int getJDBCMinorVersion() throws SQLException
    {
        return 0;
    }

    public int getMaxBinaryLiteralLength() throws SQLException
    {
        // Cassandra can represent a 2GB value, but CQL has to encode it in hex
        return Integer.MAX_VALUE / 2;
    }

    public int getMaxCatalogNameLength() throws SQLException
    {
        return Short.MAX_VALUE;
    }

    public int getMaxCharLiteralLength() throws SQLException
    {
        return Integer.MAX_VALUE;
    }

    public int getMaxColumnNameLength() throws SQLException
    {
        return Short.MAX_VALUE;
    }

    public int getMaxColumnsInGroupBy() throws SQLException
    {
        return 0;
    }

    public int getMaxColumnsInIndex() throws SQLException
    {
        return 0;
    }

    public int getMaxColumnsInOrderBy() throws SQLException
    {
        return 0;
    }

    public int getMaxColumnsInSelect() throws SQLException
    {
        return 0;
    }

    public int getMaxColumnsInTable() throws SQLException
    {
        return 0;
    }

    public int getMaxConnections() throws SQLException
    {
        return 0;
    }

    public int getMaxCursorNameLength() throws SQLException
    {
        return 0;
    }

    public int getMaxIndexLength() throws SQLException
    {
        return 0;
    }

    public int getMaxProcedureNameLength() throws SQLException
    {
        return 0;
    }

    public int getMaxRowSize() throws SQLException
    {
        return 0;
    }

    public int getMaxSchemaNameLength() throws SQLException
    {
        return 0;
    }

    public int getMaxStatementLength() throws SQLException
    {
        return 0;
    }

    public int getMaxStatements() throws SQLException
    {
        return 0;
    }

    public int getMaxTableNameLength() throws SQLException
    {
        return 0;
    }

    public int getMaxTablesInSelect() throws SQLException
    {
        return 0;
    }

    public int getMaxUserNameLength() throws SQLException
    {
        return 0;
    }

    public String getNumericFunctions() throws SQLException
    {
        return null;
    }

    public ResultSet getPrimaryKeys(String arg0, String arg1, String arg2) throws SQLException
    {
        return new CResultSet();
    }

    public ResultSet getProcedureColumns(String arg0, String arg1, String arg2, String arg3) throws SQLException
    {
        return new CResultSet();
    }

    public String getProcedureTerm() throws SQLException
    {
        return "";
    }

    public ResultSet getProcedures(String arg0, String arg1, String arg2) throws SQLException
    {
        return new CResultSet();
    }

    public int getResultSetHoldability() throws SQLException
    {
        return CResultSet.DEFAULT_HOLDABILITY;
    }

    public RowIdLifetime getRowIdLifetime() throws SQLException
    {
        return RowIdLifetime.ROWID_VALID_FOREVER;
    }

    public String getSQLKeywords() throws SQLException
    {
        return "";
    }

    public int getSQLStateType() throws SQLException
    {
        return sqlStateSQL;
    }

    public String getSchemaTerm() throws SQLException
    {
        return "";
    }

    public ResultSet getSchemas() throws SQLException
    {
        return new CResultSet();
    }

    public ResultSet getSchemas(String arg0, String arg1) throws SQLException
    {
        return new CResultSet();
    }

    public String getSearchStringEscape() throws SQLException
    {
        return "\\";
    }

    public String getStringFunctions() throws SQLException
    {
        return "";
    }

    public ResultSet getSuperTables(String arg0, String arg1, String arg2) throws SQLException
    {
        return new CResultSet();
    }

    public ResultSet getSuperTypes(String arg0, String arg1, String arg2) throws SQLException
    {
        return new CResultSet();
    }

    public String getSystemFunctions() throws SQLException
    {
        return "";
    }

    public ResultSet getTablePrivileges(String arg0, String arg1, String arg2) throws SQLException
    {
        return new CResultSet();
    }

    public ResultSet getTableTypes() throws SQLException
    {
        return new CResultSet();
    }

    public ResultSet getTables(String arg0, String arg1, String arg2, String[] arg3) throws SQLException
    {
        return new CResultSet();
    }

    public String getTimeDateFunctions() throws SQLException
    {
        return "";
    }

    public ResultSet getTypeInfo() throws SQLException
    {
        return new CResultSet();
    }

    public ResultSet getUDTs(String arg0, String arg1, String arg2, int[] arg3) throws SQLException
    {
        return new CResultSet();
    }

    public String getURL() throws SQLException
    {
        return connection.url;
    }

    public String getUserName() throws SQLException
    {
        return (connection.username==null) ? "" : connection.username;
    }

    public ResultSet getVersionColumns(String arg0, String arg1, String arg2) throws SQLException
    {
        return new CResultSet();
    }

    public boolean insertsAreDetected(int arg0) throws SQLException
    {
        return false;
    }

    public boolean isCatalogAtStart() throws SQLException
    {
        return false;
    }

    public boolean isReadOnly() throws SQLException
    {
        return false;
    }

    public boolean locatorsUpdateCopy() throws SQLException
    {
        return false;
    }

    public boolean nullPlusNonNullIsNull() throws SQLException
    {
        return false;
    }

    public boolean nullsAreSortedAtEnd() throws SQLException
    {
        return false;
    }

    public boolean nullsAreSortedAtStart() throws SQLException
    {
        return true;
    }

    public boolean nullsAreSortedHigh() throws SQLException
    {
        return true;
    }

    public boolean nullsAreSortedLow() throws SQLException
    {

        return false;
    }

    public boolean othersDeletesAreVisible(int arg0) throws SQLException
    {
        return false;
    }

    public boolean othersInsertsAreVisible(int arg0) throws SQLException
    {
        return false;
    }

    public boolean othersUpdatesAreVisible(int arg0) throws SQLException
    {
        return false;
    }

    public boolean ownDeletesAreVisible(int arg0) throws SQLException
    {
        return false;
    }

    public boolean ownInsertsAreVisible(int arg0) throws SQLException
    {
        return false;
    }

    public boolean ownUpdatesAreVisible(int arg0) throws SQLException
    {
        return false;
    }

    public boolean storesLowerCaseIdentifiers() throws SQLException
    {
        return false;
    }

    public boolean storesLowerCaseQuotedIdentifiers() throws SQLException
    {
        return false;
    }

    public boolean storesMixedCaseIdentifiers() throws SQLException
    {
        return true;
    }

    public boolean storesMixedCaseQuotedIdentifiers() throws SQLException
    {
        return true;
    }

    public boolean storesUpperCaseIdentifiers() throws SQLException
    {
        return false;
    }

    public boolean storesUpperCaseQuotedIdentifiers() throws SQLException
    {
        return false;
    }

    public boolean supportsANSI92EntryLevelSQL() throws SQLException
    {
        return false;
    }

    public boolean supportsANSI92FullSQL() throws SQLException
    {
        return false;
    }

    public boolean supportsANSI92IntermediateSQL() throws SQLException
    {
        return false;
    }

    public boolean supportsAlterTableWithAddColumn() throws SQLException
    {
        return true;
    }

    public boolean supportsAlterTableWithDropColumn() throws SQLException
    {
        return true;
    }

    public boolean supportsBatchUpdates() throws SQLException
    {
        return false;
    }

    public boolean supportsCatalogsInDataManipulation() throws SQLException
    {
        return false;
    }

    public boolean supportsCatalogsInIndexDefinitions() throws SQLException
    {
        return false;
    }

    public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException
    {
        return false;
    }

    public boolean supportsCatalogsInProcedureCalls() throws SQLException
    {
        return false;
    }

    public boolean supportsCatalogsInTableDefinitions() throws SQLException
    {
        return false;
    }

    public boolean supportsColumnAliasing() throws SQLException
    {
        return false;
    }

    public boolean supportsConvert() throws SQLException
    {
        return false;
    }

    public boolean supportsConvert(int arg0, int arg1) throws SQLException
    {
        return false;
    }

    public boolean supportsCoreSQLGrammar() throws SQLException
    {
        return false;
    }

    public boolean supportsCorrelatedSubqueries() throws SQLException
    {
        return false;
    }

    public boolean supportsDataDefinitionAndDataManipulationTransactions() throws SQLException
    {
        return false;
    }

    public boolean supportsDataManipulationTransactionsOnly() throws SQLException
    {
        return false;
    }

    public boolean supportsDifferentTableCorrelationNames() throws SQLException
    {
        return false;
    }

    public boolean supportsExpressionsInOrderBy() throws SQLException
    {
        return false;
    }

    public boolean supportsExtendedSQLGrammar() throws SQLException
    {
        return false;
    }

    public boolean supportsFullOuterJoins() throws SQLException
    {
        return false;
    }

    public boolean supportsGetGeneratedKeys() throws SQLException
    {
        return false;
    }

    public boolean supportsGroupBy() throws SQLException
    {
        return false;
    }

    public boolean supportsGroupByBeyondSelect() throws SQLException
    {
        return false;
    }

    public boolean supportsGroupByUnrelated() throws SQLException
    {
        return false;
    }

    public boolean supportsIntegrityEnhancementFacility() throws SQLException
    {
        return false;
    }

    public boolean supportsLikeEscapeClause() throws SQLException
    {

        return false;
    }

    public boolean supportsLimitedOuterJoins() throws SQLException
    {
        return false;
    }

    public boolean supportsMinimumSQLGrammar() throws SQLException
    {
        return false;
    }

    public boolean supportsMixedCaseIdentifiers() throws SQLException
    {
        return true;
    }

    public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException
    {
        return true;
    }

    public boolean supportsMultipleOpenResults() throws SQLException
    {
        return false;
    }

    public boolean supportsMultipleResultSets() throws SQLException
    {
        return false;
    }

    public boolean supportsMultipleTransactions() throws SQLException
    {
        return false;
    }

    public boolean supportsNamedParameters() throws SQLException
    {
        return false;
    }

    public boolean supportsNonNullableColumns() throws SQLException
    {

        return false;
    }

    public boolean supportsOpenCursorsAcrossCommit() throws SQLException
    {
        return false;
    }

    public boolean supportsOpenCursorsAcrossRollback() throws SQLException
    {
        return false;
    }

    public boolean supportsOpenStatementsAcrossCommit() throws SQLException
    {
        return false;
    }

    public boolean supportsOpenStatementsAcrossRollback() throws SQLException
    {
        return false;
    }

    public boolean supportsOrderByUnrelated() throws SQLException
    {
        return false;
    }

    public boolean supportsOuterJoins() throws SQLException
    {
        return false;
    }

    public boolean supportsPositionedDelete() throws SQLException
    {
        return false;
    }

    public boolean supportsPositionedUpdate() throws SQLException
    {
        return false;
    }

    public boolean supportsResultSetConcurrency(int arg0, int arg1) throws SQLException
    {
        return false;
    }

    public boolean supportsResultSetHoldability(int holdability) throws SQLException
    {

        return ResultSet.HOLD_CURSORS_OVER_COMMIT==holdability;
    }

    public boolean supportsResultSetType(int type) throws SQLException
    {

        return ResultSet.TYPE_FORWARD_ONLY==type;
    }

    public boolean supportsSavepoints() throws SQLException
    {
        return false;
    }

    public boolean supportsSchemasInDataManipulation() throws SQLException
    {
        return false;
    }

    public boolean supportsSchemasInIndexDefinitions() throws SQLException
    {
        return false;
    }

    public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException
    {
        return false;
    }

    public boolean supportsSchemasInProcedureCalls() throws SQLException
    {
        return false;
    }

    public boolean supportsSchemasInTableDefinitions() throws SQLException
    {
        return false;
    }

    public boolean supportsSelectForUpdate() throws SQLException
    {
        return false;
    }

    public boolean supportsStatementPooling() throws SQLException
    {
        return false;
    }

    public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException
    {
        return false;
    }

    public boolean supportsStoredProcedures() throws SQLException
    {
        return false;
    }

    public boolean supportsSubqueriesInComparisons() throws SQLException
    {
        return false;
    }

    public boolean supportsSubqueriesInExists() throws SQLException
    {
        return false;
    }

    public boolean supportsSubqueriesInIns() throws SQLException
    {
        return false;
    }

    public boolean supportsSubqueriesInQuantifieds() throws SQLException
    {
        return false;
    }

    public boolean supportsTableCorrelationNames() throws SQLException
    {
        return false;
    }

    public boolean supportsTransactionIsolationLevel(int level) throws SQLException
    {

        return Connection.TRANSACTION_NONE==level;
    }

    public boolean supportsTransactions() throws SQLException
    {
        return false;
    }

    public boolean supportsUnion() throws SQLException
    {
        return false;
    }

    public boolean supportsUnionAll() throws SQLException
    {
        return false;
    }

    public boolean updatesAreDetected(int arg0) throws SQLException
    {
        return false;
    }

    public boolean usesLocalFilePerTable() throws SQLException
    {
        return false;
    }

    public boolean usesLocalFiles() throws SQLException
    {
        return false;
    }

}
