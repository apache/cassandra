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
package org.apache.cassandra.cli;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.IndexOperator;
import org.apache.cassandra.thrift.KsDef;
import org.apache.commons.lang3.StringEscapeUtils;

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
        if (b.charAt(0) == '\'' && b.charAt(b.length()-1) == '\'')
            b = b.substring(1, b.length()-1);
        return StringEscapeUtils.unescapeJava(b);
    }

    public static String escapeSQLString(String b)
    {
        // single quotes are not escaped in java, need to be for cli
        return StringEscapeUtils.escapeJava(b).replace("\'", "\\'");
    }

    public static String maybeEscapeName(String name)
    {
        return Character.isLetter(name.charAt(0)) ? name : "\'" + name + "\'";
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

    /**
     * Parse the statement from cli and return KsDef
     *
     * @param keyspaceName - name of the keyspace to lookup
     * @param keyspaces - List of known keyspaces
     *
     * @return metadata about keyspace or null
     */
    public static KsDef getKeySpaceDef(String keyspaceName, List<KsDef> keyspaces)
    {
        keyspaceName = keyspaceName.toUpperCase();

        for (KsDef ksDef : keyspaces)
        {
            if (ksDef.name.toUpperCase().equals(keyspaceName))
                return ksDef;
        }

        return null;
    }

    public static String quote(String str)
    {
        return String.format("'%s'", str);
    }
}
