/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.cql.driver;

import org.apache.cassandra.cql.compiler.common.*;
import org.apache.cassandra.cql.compiler.parse.*;
import org.apache.cassandra.cql.compiler.sem.*;
import org.apache.cassandra.cql.common.*;
import com.facebook.thrift.*;

import org.apache.cassandra.cql.common.CqlResult;
import org.apache.cassandra.cql.common.Plan;
import org.apache.cassandra.cql.compiler.common.CqlCompiler;
import org.apache.cassandra.cql.compiler.parse.ParseException;
import org.apache.cassandra.cql.compiler.sem.SemanticException;
import org.apache.cassandra.utils.LogUtil;
import org.apache.log4j.Logger;

// Server side driver class for CQL
public class CqlDriver 
{
    private final static Logger logger_ = Logger.getLogger(CqlDriver.class);

    // Execute a CQL Statement 
    public static CqlResult executeQuery(String query) throws TException 
    {
        CqlCompiler compiler = new CqlCompiler();

        try
        {
            logger_.debug("Compiling CQL query ...");
            Plan plan = compiler.compileQuery(query);
            if (plan != null)
            {
                logger_.debug("Executing CQL query ...");            
                return plan.execute();
            }
        }
        catch (Exception e)
        {
            CqlResult result = new CqlResult(null);
            result.errorTxt = e.getMessage();           

            Class<? extends Exception> excpClass = e.getClass();
            if ((excpClass != SemanticException.class)
                && (excpClass != ParseException.class)
                && (excpClass != RuntimeException.class))
            {
                result.errorTxt = "CQL Internal Error: " + result.errorTxt;
                result.errorCode = 1; // failure
                logger_.error(LogUtil.throwableToString(e));
            }

            return result;
        }

        return null;
    }
}
