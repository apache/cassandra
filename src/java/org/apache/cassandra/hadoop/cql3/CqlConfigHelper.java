package org.apache.cassandra.hadoop.cql3;
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
import org.apache.hadoop.conf.Configuration;

public class CqlConfigHelper
{
    private static final String INPUT_CQL_COLUMNS_CONFIG = "cassandra.input.columnfamily.columns"; // separate by colon ,
    private static final String INPUT_CQL_PAGE_ROW_SIZE_CONFIG = "cassandra.input.page.row.size";
    private static final String INPUT_CQL_WHERE_CLAUSE_CONFIG = "cassandra.input.where.clause";
    private static final String OUTPUT_CQL = "cassandra.output.cql";

    /**
     * Set the CQL columns for the input of this job.
     *
     * @param conf Job configuration you are about to run
     * @param columns
     */
    public static void setInputColumns(Configuration conf, String columns)
    {
        if (columns == null || columns.isEmpty())
            return;
        
        conf.set(INPUT_CQL_COLUMNS_CONFIG, columns);
    }
    
    /**
     * Set the CQL query Limit for the input of this job.
     *
     * @param conf Job configuration you are about to run
     * @param cqlPageRowSize
     */
    public static void setInputCQLPageRowSize(Configuration conf, String cqlPageRowSize)
    {
        if (cqlPageRowSize == null)
        {
            throw new UnsupportedOperationException("cql page row size may not be null");
        }

        conf.set(INPUT_CQL_PAGE_ROW_SIZE_CONFIG, cqlPageRowSize);
    }

    /**
     * Set the CQL user defined where clauses for the input of this job.
     *
     * @param conf Job configuration you are about to run
     * @param clauses
     */
    public static void setInputWhereClauses(Configuration conf, String clauses)
    {
        if (clauses == null || clauses.isEmpty())
            return;
        
        conf.set(INPUT_CQL_WHERE_CLAUSE_CONFIG, clauses);
    }
  
    /**
     * Set the CQL prepared statement for the output of this job.
     *
     * @param conf Job configuration you are about to run
     * @param cql
     */
    public static void setOutputCql(Configuration conf, String cql)
    {
        if (cql == null || cql.isEmpty())
            return;
        
        conf.set(OUTPUT_CQL, cql);
    }
    
    
    public static String getInputcolumns(Configuration conf)
    {
        return conf.get(INPUT_CQL_COLUMNS_CONFIG);
    }
    
    public static String getInputPageRowSize(Configuration conf)
    {
        return conf.get(INPUT_CQL_PAGE_ROW_SIZE_CONFIG);
    }
    
    public static String getInputWhereClauses(Configuration conf)
    {
        return conf.get(INPUT_CQL_WHERE_CLAUSE_CONFIG);
    }
    
    public static String getOutputCql(Configuration conf)
    {
        return conf.get(OUTPUT_CQL);
    }
}
