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
package org.apache.cassandra.cql.driver.jdbc;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Properties;

/**
  * The Class CassandraDriver.
  */
 public class CassandraDriver implements Driver
{
    
    /** The Constant MAJOR_VERSION. */
    private static final int MAJOR_VERSION = 1;
    
    /** The Constant MINOR_VERSION. */
    private static final int MINOR_VERSION = 0;

    /** The ACCEPT s_ url. */
    private static String ACCEPTS_URL = "jdbc:cassandra";
    
//    private static final Logger logger = LoggerFactory.getLogger(CassandraDriver.class); 

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
            throw new DriverResolverException(e.getMessage());
        }
    }

    
    /**
     * Method to validate whether provided connection url matches with pattern or not.
     *
     * @param url  connection url.
     * @return true, if successful
     * @throws SQLException the sQL exception
     */
    public boolean acceptsURL(String url) throws SQLException
    {
        return url.startsWith(ACCEPTS_URL);
    }

    /**
     * Method to return connection instance for given connection url and connection props.
     *
     * @param url               connection url.
     * @param props          connection properties.
     * @return connection connection instance.
     * @throws SQLException the sQL exception
     */
    public Connection connect(String url, Properties props) throws SQLException
    {
        if (acceptsURL(url))
        {
            return new CassandraConnection(url);
        }
        else
        {
            throw new InvalidUrlException("Invalid connection url:" + url + ". should start with jdbc:cassandra");
        }
    }

    /**
     * Returns default major version.
     * @return MAJOR_VERSION major version.
     */
    public int getMajorVersion()
    {
        return MAJOR_VERSION;
    }

    /**
     * Returns default minor version.
     * @return MINOR_VERSION minor version.
     */
    public int getMinorVersion()
    {
        return MINOR_VERSION;
    }

    /**
     * Returns default driver property info object.
     *
     * @param arg0 the arg0
     * @param arg1 the arg1
     * @return driverPropertyInfo
     * @throws SQLException the sQL exception
     */
    public DriverPropertyInfo[] getPropertyInfo(String arg0, Properties arg1) throws SQLException
    {
        return new DriverPropertyInfo[0];
    }

   /**
    * Returns true, if it is jdbc compliant.    
    * @return value true, if it is jdbc compliant.
    */
    public boolean jdbcCompliant()
    {
        return false;
    }

}
