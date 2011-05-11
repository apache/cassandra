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


import java.math.BigInteger;
import java.sql.ResultSet;

public interface CassandraResultSet extends ResultSet
{
    /**
     * @return the current row key
     */
    public byte[] getKey();

    /** @return a BigInteger value for the given column offset*/
    public BigInteger getBigInteger(int i);
    /** @return a BigInteger value for the given column name */
    public BigInteger getBigInteger(String name);

    /** @return the raw column data for the given column offset */
    public TypedColumn getColumn(int i);
    /** @return the raw column data for the given column name */
    public TypedColumn getColumn(String name);
}
