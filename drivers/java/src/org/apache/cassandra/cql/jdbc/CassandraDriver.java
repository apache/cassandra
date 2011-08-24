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

import static org.apache.cassandra.cql.jdbc.Utils.PROTOCOL;
import static org.apache.cassandra.cql.jdbc.Utils.TAG_PASSWORD;
import static org.apache.cassandra.cql.jdbc.Utils.TAG_USER;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The Class CassandraDriver.
 */
public class CassandraDriver implements Driver
{
    public static final int DVR_MAJOR_VERSION = 1;

    public static final int DVR_MINOR_VERSION = 0;

    public static final int DVR_PATCH_VERSION = 4;

    public static final String DVR_NAME = "Cassandra JDBC Driver";

    private static final Logger logger = LoggerFactory.getLogger(CassandraDriver.class);

    static
    {
        // Register the CassandraDriver with DriverManager
        try
        {
            CassandraDriver driverInst = new CassandraDriver();
            DriverManager.registerDriver(driverInst);
        }
        catch (SQLException e)
        {
            throw new RuntimeException(e.getMessage());
        }
    }

    /**
     * Method to validate whether provided connection url matches with pattern or not.
     */
    public boolean acceptsURL(String url) throws SQLException
    {
        return url.startsWith(PROTOCOL);
    }

    /**
     * Method to return connection instance for given connection url and connection props.
     */
    public Connection connect(String url, Properties props) throws SQLException
    {
        Properties finalProps;
        if (acceptsURL(url))
        {
            // parse the URL into a set of Properties
            finalProps = Utils.parseURL(url);

            // override any matching values in finalProps with values from props
            finalProps.putAll(props);

            if (logger.isDebugEnabled()) logger.debug("Final Properties to Connection: {}", finalProps);

            return new CassandraConnection(finalProps);
        }
        else
        {
            return null; // signal it is the wrong driver for this protocol:subprotocol
        }
    }

    /**
     * Returns default major version.
     */
    public int getMajorVersion()
    {
        return DVR_MAJOR_VERSION;
    }

    /**
     * Returns default minor version.
     */
    public int getMinorVersion()
    {
        return DVR_MINOR_VERSION;
    }

    /**
     * Returns default driver property info object.
     */
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties props) throws SQLException
    {
        if (props == null) props = new Properties();

        DriverPropertyInfo[] info = new DriverPropertyInfo[2];

        info[0] = new DriverPropertyInfo(TAG_USER, props.getProperty(TAG_USER));
        info[0].description = "The 'user' property";

        info[1] = new DriverPropertyInfo(TAG_PASSWORD, props.getProperty(TAG_PASSWORD));
        info[1].description = "The 'password' property";

        return info;
    }

    /**
     * Returns true, if it is jdbc compliant.
     */
    public boolean jdbcCompliant()
    {
        return false;
    }
}
