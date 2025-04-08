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

package org.apache.cassandra.harry.execution;

import org.apache.cassandra.distributed.api.ConsistencyLevel;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.UUID;

public class CompiledStatement
{
    public final boolean validating;
    private final String cql;
    private final Object[] bindings;
    private ByteBuffer[] pk;

    public CompiledStatement(boolean validating, String cql, Object... bindings)
    {
        this.validating = validating;
        this.cql = cql;
        this.bindings = bindings;
    }

    public String cql()
    {
        return cql;
    }

    public CompiledStatement withSchema(String oldKs, String oldTable, String newKs, String newTable)
    {
        CompiledStatement statement = new CompiledStatement(validating, cql.replace(oldKs + "." + oldTable,
                newKs + "." + newTable),
                bindings);
        statement.setPk(pk);
        return statement;
    }

    public CompiledStatement withFiltering()
    {
        return new CompiledStatement(validating, cql.replace(";",
                                                 " ALLOW FILTERING;"),
                                     bindings);
    }

    public void setPk(ByteBuffer[] pk)
    {
        this.pk = pk;
    }

    public ByteBuffer[] pk()
    {
        return pk;
    }

    public Object[] bindings()
    {
        return bindings;
    }

    public String toString()
    {
        return "CompiledStatement{" +
               "cql=execute(\"" + cql.replace("\n", "\" + \n\"") + '\"' +
               ", " + bindingsToString(bindings) +
               '}';
    }

    public String dump(ConsistencyLevel cl)
    {
        return String.format("cluster.coordinator(1).execute(\"%s\", ConsistencyLevel.%s, %s);",
                             cql.replace("\n", "\" + \n\""),
                             cl.toString(),
                             bindingsToString(bindings));
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
            else if (binding instanceof Double)
                sb.append("(double)").append(binding);
            else if (binding instanceof Long)
                sb.append(binding).append("L");
            else if (binding instanceof Integer)
                sb.append("(int)").append(binding);
            else if (binding instanceof Boolean)
                sb.append(binding);
            else if (binding instanceof UUID)
                sb.append("java.util.UUID.fromString(\"").append(binding).append("\")");
            else if (binding instanceof java.sql.Timestamp)
                sb.append("new java.sql.Timestamp(").append(((java.sql.Timestamp) binding).getTime()).append("L)");
            else if (binding instanceof java.sql.Time)
                sb.append("new java.sql.Time(").append(((java.sql.Time) binding).getTime()).append("L)");
            else if (binding instanceof java.util.Date)
            {
                sb.append("new java.util.Date(")
                  .append(((java.util.Date) binding).getTime())
                  .append("L)");
            }
            else if (binding instanceof java.math.BigInteger)
                sb.append("new java.math.BigInteger(\"").append(binding).append("\")");
            else if (binding instanceof java.math.BigDecimal)
                sb.append("new java.math.BigDecimal(\"").append(binding).append("\")");
            else if (binding instanceof java.net.InetAddress)
            {
                byte[] address = ((InetAddress) binding).getAddress();
                sb.append("java.net.InetAddress.getByAddress(new byte[]{");
                for (int i = 0; i < address.length; i++) {
                    sb.append(address[i]);
                    if (i < address.length - 1) sb.append(", ");
                }
                sb.append("})");
            }
            else
                sb.append(binding);
            // TODO: byte arrays
        }
        return sb.toString();
    }
}
