/*
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

package org.apache.cassandra.cql3.udf;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.UFMetaData;
import org.apache.cassandra.cql3.AssignementTestable;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.cql3.functions.Functions;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.exceptions.InvalidRequestException;

/**
 * Central registry for user defined functions (CASSANDRA-7395).
 * <p/>
 * UDFs are maintained in {@code system.schema_functions} table and distributed to all nodes.
 * <p/>
 * UDFs are not maintained in {@link org.apache.cassandra.cql3.functions.Functions} class to have a strict
 * distinction between 'core CQL' functions provided by Cassandra and functions provided by the user.
 * 'Core CQL' functions have precedence over UDFs.
 */
public class UDFRegistry
{
    private static final Logger logger = LoggerFactory.getLogger(UDFRegistry.class);

    static final String SELECT_CQL = "SELECT namespace, name, signature, deterministic, argument_names, argument_types, " +
                                     "return_type, language, body FROM " +
                                     Keyspace.SYSTEM_KS + '.' + SystemKeyspace.SCHEMA_FUNCTIONS_CF;

    private static final Map<String, UDFFunctionOverloads> functions = new ConcurrentHashMap<>();

    public static void init()
    {
        refreshInitial();
    }

    /**
     * Initial loading of all existing UDFs.
     */
    public static void refreshInitial()
    {
        logger.debug("Refreshing UDFs");
        for (UntypedResultSet.Row row : QueryProcessor.executeOnceInternal(SELECT_CQL))
        {
            UFMetaData uf = UFMetaData.fromSchema(row);
            UDFFunctionOverloads sigMap = functions.get(uf.qualifiedName);
            if (sigMap == null)
                functions.put(uf.qualifiedName, sigMap = new UDFFunctionOverloads());

            if (Functions.contains(uf.qualifiedName))
                logger.warn("The UDF '" + uf.functionName + "' cannot be used because it uses the same name as the CQL " +
                            "function with the same name. You should drop this function but can do a " +
                            "'DESCRIBE FUNCTION "+uf.functionName+";' in cqlsh before to get more information about it.");

            // add the function to the registry even if it is invalid (to be able to drop it)
            sigMap.addAndInit(uf, true);

            if (uf.invalid != null)
                logger.error("Loaded invalid UDF : " + uf.invalid.getMessage());
        }
    }

    public static boolean hasFunction(String qualifiedName)
    {
        UDFFunctionOverloads sigMap = functions.get(qualifiedName.toLowerCase());
        return sigMap != null && !sigMap.isEmpty();
    }

    public static UDFunction resolveFunction(String namespace, String functionName, String ksName, String cfName,
                                             List<? extends AssignementTestable> args)
    throws InvalidRequestException
    {
        UDFFunctionOverloads sigMap = functions.get(UFMetaData.qualifiedName(namespace, functionName));
        if (sigMap != null)
            return sigMap.resolveFunction(ksName, cfName, args);
        return null;
    }

    public static void migrateDropFunction(UFMetaData uf)
    {
        UDFFunctionOverloads sigMap = functions.get(uf.qualifiedName);
        if (sigMap == null)
            return;

        sigMap.remove(uf);
    }

    public static void migrateUpdateFunction(UFMetaData uf)
    {
        migrateAddFunction(uf);
    }

    public static void migrateAddFunction(UFMetaData uf)
    {
        addFunction(uf, true);
    }

    /**
     * Used by {@link org.apache.cassandra.cql3.statements.CreateFunctionStatement} to create or replace a new function.
     */
    public static void tryCreateFunction(UFMetaData ufMeta) throws InvalidRequestException
    {
        addFunction(ufMeta, false);

        if (ufMeta.invalid != null)
            throw ufMeta.invalid;
    }

    private static void addFunction(UFMetaData uf, boolean addIfInvalid)
    {
        UDFFunctionOverloads sigMap = functions.get(uf.qualifiedName);
        if (sigMap == null)
            functions.put(uf.qualifiedName, sigMap = new UDFFunctionOverloads());

        sigMap.addAndInit(uf, addIfInvalid);
    }

    public static UDFFunctionOverloads getFunctionSigMap(String qualifiedName)
    {
        return functions.get(qualifiedName);
    }
}
