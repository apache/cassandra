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

import java.io.ByteArrayOutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.SQLSyntaxErrorException;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.Deflater;

import org.apache.cassandra.thrift.Compression;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;

/**
 * A set of static utility methods used by the JDBC Suite, and various default values and error message strings
 * that can be shared across classes.
 */
class Utils
{
    private static final Pattern KEYSPACE_PATTERN = Pattern.compile("USE (\\w+);?", Pattern.CASE_INSENSITIVE | Pattern.MULTILINE);
    private static final Pattern SELECT_PATTERN = Pattern.compile("(?:SELECT|DELETE)\\s+.+\\s+FROM\\s+(\\w+).*", Pattern.CASE_INSENSITIVE | Pattern.MULTILINE);
    private static final Pattern UPDATE_PATTERN = Pattern.compile("UPDATE\\s+(\\w+)\\s+.*", Pattern.CASE_INSENSITIVE);

    public static final String PROTOCOL = "jdbc:cassandra:";
    public static final String DEFAULT_HOST = "localhost";
    public static final int DEFAULT_PORT = 9160;

    public static final String TAG_DESCRIPTION = "description";
    public static final String TAG_USER = "user";
    public static final String TAG_PASSWORD = "password";
    public static final String TAG_DATABASE_NAME = "databaseName";
    public static final String TAG_SERVER_NAME = "serverName";
    public static final String TAG_PORT_NUMBER = "portNumber";

    protected static final String WAS_CLOSED_CON = "method was called on a closed Connection";
    protected static final String WAS_CLOSED_STMT = "method was called on a closed Statement";
    protected static final String WAS_CLOSED_RSLT = "method was called on a closed ResultSet";
    protected static final String NO_INTERFACE = "no object was found that matched the provided interface: %s";
    protected static final String NO_TRANSACTIONS = "the Cassandra implementation does not support transactions";
    protected static final String NO_SERVER = "no Cassandra server is available";
    protected static final String ALWAYS_AUTOCOMMIT = "the Cassandra implementation is always in auto-commit mode";
    protected static final String BAD_TIMEOUT = "the timeout value was less than zero";
    protected static final String SCHEMA_MISMATCH = "schema does not match across nodes, (try again later)";
    protected static final String NOT_SUPPORTED = "the Cassandra implementation does not support this method";
    protected static final String NO_GEN_KEYS = "the Cassandra implementation does not currently support returning generated  keys";
    protected static final String NO_BATCH = "the Cassandra implementation does not currently support this batch in Statement";
    protected static final String NO_MULTIPLE = "the Cassandra implementation does not currently support multiple open Result Sets";
    protected static final String NO_VALIDATOR = "Could not find key validator for: %s.%s";
    protected static final String NO_COMPARATOR = "Could not find key comparator for: %s.%s";
    protected static final String NO_RESULTSET = "No ResultSet returned from the CQL statement passed in an 'executeQuery()' method";
    protected static final String NO_UPDATE_COUNT = "No Update Count was returned from the CQL statement passed in an 'executeUpdate()' method";
    protected static final String NO_CF = "no column family reference could be extracted from the provided CQL statement";
    protected static final String BAD_KEEP_RSET = "the argument for keeping the current result set : %s is not a valid value";
    protected static final String BAD_TYPE_RSET = "the argument for result set type : %s is not a valid value";
    protected static final String BAD_CONCUR_RSET = "the argument for result set concurrency : %s is not a valid value";
    protected static final String BAD_HOLD_RSET = "the argument for result set holdability : %s is not a valid value";
    protected static final String BAD_FETCH_DIR = "fetch direction value of : %s is illegal";
    protected static final String BAD_AUTO_GEN = "auto key generation value of : %s is illegal";
    protected static final String BAD_FETCH_SIZE = "fetch size of : %s rows may not be negative";
    protected static final String MUST_BE_POSITIVE = "index must be a positive number less or equal the count of returned columns: %s";
    protected static final String VALID_LABELS = "name provided was not in the list of valid column labels: %s";
    protected static final String NOT_TRANSLATABLE = "column was stored in %s format which is not translatable to %s";
    protected static final String NOT_BOOLEAN = "string value was neither 'true' nor 'false' :  %s";
    protected static final String HOST_IN_URL = "Connection url must specify a host, e.g., jdbc:cassandra://localhost:9170/Keyspace1";
    protected static final String HOST_REQUIRED = "a 'host' name is required to build a Connection";
    protected static final String BAD_KEYSPACE = "Keyspace names must be composed of alphanumerics and underscores (parsed: '%s')";
    protected static final String URI_IS_SIMPLE = "Connection url may only include host, port, and keyspace, e.g., jdbc:cassandra://localhost:9170/Keyspace1";

    protected static final Logger logger = LoggerFactory.getLogger(Utils.class);

    /**
     * Use the Compression object method to deflate the query string
     *
     * @param queryStr An un-compressed CQL query string
     * @param compression The compression object
     * @return A compressed string
     */
    public static ByteBuffer compressQuery(String queryStr, Compression compression)
    {
        byte[] data = queryStr.getBytes(Charsets.UTF_8);
        Deflater compressor = new Deflater();
        compressor.setInput(data);
        compressor.finish();

        ByteArrayOutputStream byteArray = new ByteArrayOutputStream();
        byte[] buffer = new byte[1024];

        while (!compressor.finished())
        {
            int size = compressor.deflate(buffer);
            byteArray.write(buffer, 0, size);
        }

        logger.trace("Compressed query statement {} bytes in length to {} bytes", data.length, byteArray.size());

        return ByteBuffer.wrap(byteArray.toByteArray());
    }

    /**
     * Parse a URL for the Cassandra JDBC Driver
     * <p/>
     * The URL must start with the Protocol: "jdbc:cassandra:"
     * The URI part(the "Subname") must contain a host and an optional port and optional keyspace name
     * ie. "//localhost:9160/Test1"
     *
     * @param url The full JDBC URL to be parsed
     * @return A list of properties that were parsed from the Subname
     * @throws SQLException
     */
    public static final Properties parseURL(String url) throws SQLException
    {
        Properties props = new Properties();

        if (!(url == null))
        {
            props.setProperty(TAG_PORT_NUMBER, "" + DEFAULT_PORT);
            String rawUri = url.substring(PROTOCOL.length());
            URI uri = null;
            try
            {
                uri = new URI(rawUri);
            }
            catch (URISyntaxException e)
            {
                throw new SQLSyntaxErrorException(e);
            }

            String host = uri.getHost();
            if (host == null) throw new SQLNonTransientConnectionException(HOST_IN_URL);
            props.setProperty(TAG_SERVER_NAME, host);

            int port = uri.getPort() >= 0 ? uri.getPort() : DEFAULT_PORT;
            props.setProperty(TAG_PORT_NUMBER, "" + port);

            String keyspace = uri.getPath();
            if ((keyspace != null) && (!keyspace.isEmpty()))
            {
                if (keyspace.startsWith("/")) keyspace = keyspace.substring(1);
                if (!keyspace.matches("[a-zA-Z]\\w+"))
                    throw new SQLNonTransientConnectionException(String.format(BAD_KEYSPACE, keyspace));
                props.setProperty(TAG_DATABASE_NAME, keyspace);
            }

            if (uri.getUserInfo() != null)
                throw new SQLNonTransientConnectionException(URI_IS_SIMPLE);
        }

        if (logger.isTraceEnabled()) logger.trace("URL : '{}' parses to: {}", url, props);

        return props;
    }

    /**
     * Create a "Subname" portion of a JDBC URL from properties.
     * 
     * @param props A Properties file containing all the properties to be considered.
     * @return A constructed "Subname" portion of a JDBC URL in the form of a CLI (ie: //myhost:9160/Test1 )
     * @throws SQLException
     */
    public static final String createSubName(Properties props)throws SQLException
    {
        // make keyspace always start with a "/" for URI
        String keyspace = props.getProperty(TAG_DATABASE_NAME);
     
        // if keyspace is null then do not bother ...
        if (keyspace != null) 
            if (!keyspace.startsWith("/")) keyspace = "/"  + keyspace;
        
        String host = props.getProperty(TAG_SERVER_NAME);
        if (host==null)throw new SQLNonTransientConnectionException(HOST_REQUIRED);
        
        // construct a valid URI from parts... 
        URI uri;
        try
        {
            uri = new URI(
                null,
                null,
                host,
                props.getProperty(TAG_PORT_NUMBER)==null ? DEFAULT_PORT : Integer.parseInt(props.getProperty(TAG_PORT_NUMBER)),
                keyspace,
                null,
                null);
        }
        catch (Exception e)
        {
            throw new SQLNonTransientConnectionException(e);
        }
        
        if (logger.isTraceEnabled()) logger.trace("Subname : '{}' created from : {}",uri.toString(), props);
        
        return uri.toString();
    }
    
    /**
     * Determine the current keyspace by inspecting the CQL string to see if a USE statement is provided; which would change the keyspace.
     *
     * @param cql     A CQL query string
     * @param current The current keyspace stored as state in the connection
     * @return the provided keyspace name or the keyspace from the contents of the CQL string
     */
    public static String determineCurrentKeyspace(String cql, String current)
    {
        String ks = current;
        Matcher isKeyspace = KEYSPACE_PATTERN.matcher(cql);
        if (isKeyspace.matches()) ks = isKeyspace.group(1);
        return ks;
    }

    /**
     * Determine the current column family by inspecting the CQL to find a CF reference.
     *
     * @param cql A CQL query string
     * @return The column family name from the contents of the CQL string or null in none was found
     */
    public static String determineCurrentColumnFamily(String cql)
    {
        String cf = null;
        Matcher isSelect = SELECT_PATTERN.matcher(cql);
        if (isSelect.matches()) cf = isSelect.group(1);
        Matcher isUpdate = UPDATE_PATTERN.matcher(cql);
        if (isUpdate.matches()) cf = isUpdate.group(1);
        return cf;
    }
}
