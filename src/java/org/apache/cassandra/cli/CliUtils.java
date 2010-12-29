package org.apache.cassandra.cli;

import java.util.LinkedHashSet;
import java.util.Set;

import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.IndexOperator;
import org.apache.cassandra.thrift.KsDef;
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


public class CliUtils
{
    /**
     * Strips leading and trailing "'" characters, and handles
     * and escaped characters such as \n, \r, etc.
     * @param b - string to unescape
     * @return String - unexspaced string
     */
    public static String unescapeSQLString(String b)
    {
        int j = 1;
        final char start = b.charAt(0);
        final char end = b.charAt(b.length() - 1);

        if (start != '\'' && end != '\'')
        {
            j = 0;
        }

        StringBuilder sb = new StringBuilder(b.length());

        for (int i = j; ((j == 0) ? i : i + 1) < b.length(); i++)
        {
            if (b.charAt(i) == '\\' && i + 2 < b.length())
            {
                char n = b.charAt(i + 1);
                switch (n)
                {
                    case '0':
                        sb.append("\0");
                        break;
                    case '\'':
                        sb.append("'");
                        break;
                    case '"':
                        sb.append("\"");
                        break;
                    case 'b':
                        sb.append("\b");
                        break;
                    case 'n':
                        sb.append("\n");
                        break;
                    case 'r':
                        sb.append("\r");
                        break;
                    case 't':
                        sb.append("\t");
                        break;
                    case 'Z':
                        sb.append("\u001A");
                        break;
                    case '\\':
                        sb.append("\\");
                        break;
                    case '%':
                        sb.append("%");
                        break;
                    case '_':
                        sb.append("_");
                        break;
                    default:
                        sb.append(n);
                }
            }
            else
            {
                sb.append(b.charAt(i));
            }
        }

        return sb.toString();
    }

    /**
     * Returns IndexOperator from string representation
     * @param operator - string representing IndexOperator (=, >=, >, <, <=)
     * @return IndexOperator - enum value of IndexOperator or null if not found
     */
    public static IndexOperator getIndexOperator(String operator)
    {
        if (operator.equals("="))
        {
            return IndexOperator.EQ;
        }
        else if (operator.equals(">="))
        {
            return IndexOperator.GTE;
        }
        else if (operator.equals(">"))
        {
            return IndexOperator.GT;
        }
        else if (operator.equals("<"))
        {
            return IndexOperator.LT;
        }
        else if (operator.equals("<="))
        {
            return IndexOperator.LTE;
        }

        return null;
    }

    /**
     * Returns set of column family names in specified keySpace.
     * @param keySpace - keyspace definition to get column family names from.
     * @return Set - column family names
     */
    public static Set<String> getCfNamesByKeySpace(KsDef keySpace)
    {
        Set<String> names = new LinkedHashSet<String>();

        for (CfDef cfDef : keySpace.getCf_defs())
        {
            names.add(cfDef.getName());
        }

        return names;
    }
}
