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

package org.apache.cassandra.harry.operations;

public class CompiledStatement
{
    private final String cql;
    private final Object[] bindings;

    public CompiledStatement(String cql, Object... bindings)
    {
        this.cql = cql;
        this.bindings = bindings;
    }

    public String cql()
    {
        return cql;
    }

    public CompiledStatement withSchema(String oldKs, String oldTable, String newKs, String newTable)
    {
        return new CompiledStatement(cql.replace(oldKs + "." + oldTable,
                                                 newKs + "." + newTable),
                                     bindings);
    }

    public CompiledStatement withFiltering()
    {
        return new CompiledStatement(cql.replace(";",
                                                 " ALLOW FILTERING;"),
                                     bindings);
    }

    public Object[] bindings()
    {
        return bindings;
    }

    public static CompiledStatement create(String cql, Object... bindings)
    {
        return new CompiledStatement(cql, bindings);
    }

    public String toString()
    {
        return "CompiledStatement{" +
               "cql='" + cql + '\'' +
               ", bindings=" + bindingsToString(bindings) +
               '}';
    }

    public static String bindingsToString(Object... bindings)
    {
        StringBuilder sb = new StringBuilder();
        boolean isFirst = true;
        for (Object binding : bindings)
        {
            if (isFirst)
                isFirst = false;
            else
                sb.append(",");

            if (binding instanceof String)
                sb.append("\"").append(binding).append("\"");
            else if (binding instanceof Short)
                sb.append("(short)").append(binding);
            else if (binding instanceof Byte)
                sb.append("(byte)").append(binding);
            else if (binding instanceof Float)
                sb.append("(float)").append(binding);
            else if (binding instanceof Long)
                sb.append(binding).append("L");
            else
                sb.append(binding);
        }
        return sb.toString();
    }
}
