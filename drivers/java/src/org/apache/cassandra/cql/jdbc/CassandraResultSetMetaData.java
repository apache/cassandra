package org.apache.cassandra.cql.jdbc;
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


import java.sql.SQLException;

/**
 * ResultSetMetaData give lots of nice detailed type inforamtion about column values.
 * This interface aims to do the same thing but distinguishes column names and values.
 */
public interface CassandraResultSetMetaData
{
    /** exposes row key */
    public byte[] getKey();
    
    // the rest of these methods have similar calls in java.sql.ResultSetMetaData.
    
    public boolean isNameCaseSensitive(int column) throws SQLException;
    public boolean isNameCurrency(int column) throws SQLException;
    public boolean isNameSigned(int column) throws SQLException;
    public int getNameDisplaySize(int column) throws SQLException;
    public int getNamePrecision(int column) throws SQLException;
    public int getNameScale(int column) throws SQLException;
    public int getNameType(int column) throws SQLException;
    public String getNameTypeName(int column) throws SQLException;
    public String getNameClassName(int column) throws SQLException;
    
    public boolean isValueCaseSensitive(int column) throws SQLException;
    public boolean isValueCurrency(int column) throws SQLException;
    public boolean isValueSigned(int column) throws SQLException;
    public int getValueDisplaySize(int column) throws SQLException;
    public int getValuePrecision(int column) throws SQLException;
    public int getValueScale(int column) throws SQLException;
    public int getValueType(int column) throws SQLException;
    public String getValueTypeName(int column) throws SQLException;
    public String getValueClassName(int column) throws SQLException;
}
