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

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.cassandra.config.UFMetaData;
import org.apache.cassandra.cql3.AssignementTestable;
import org.apache.cassandra.exceptions.InvalidRequestException;

public final class UDFFunctionOverloads
{
    final Map<String, UFMetaData> signatureMap = new ConcurrentHashMap<>();
    final Map<String, UDFunction> udfInstances = new ConcurrentHashMap<>();

    public void addAndInit(UFMetaData uf, boolean addIfInvalid)
    {
        try
        {
            UDFunction UDFunction = new UDFunction(uf);
            udfInstances.put(uf.signature, UDFunction);
        }
        catch (InvalidRequestException e)
        {
            uf.invalid = e;
        }

        if (uf.invalid == null || addIfInvalid)
            signatureMap.put(uf.signature, uf);
    }

    public void remove(UFMetaData uf)
    {
        signatureMap.remove(uf.signature);
        udfInstances.remove(uf.signature);
    }

    public Collection<UFMetaData> values()
    {
        return signatureMap.values();
    }

    public boolean isEmpty()
    {
        return signatureMap.isEmpty();
    }

    public UDFunction resolveFunction(String ksName, String cfName, List<? extends AssignementTestable> args)
    throws InvalidRequestException
    {
        for (UFMetaData candidate : signatureMap.values())
        {
            // Currently the UDF implementation must use concrete types (like Double, Integer) instead of base types (like Number).
            // To support handling of base types it is necessary to construct new, temporary instances of UDFFunction with the
            // signature for the current request in UDFFunction#argsType + UDFFunction#returnType.
            // Additionally we need the requested return type (AssignementTestable) has a parameter for this method.
            if (candidate.compatibleArgs(ksName, cfName, args))
            {

                // TODO CASSANDRA-7557 (specific per-function EXECUTE permission ??)

                if (candidate.invalid != null)
                    throw new InvalidRequestException(candidate.invalid.getMessage());
                return udfInstances.get(candidate.signature);
            }
        }
        return null;
    }
}
