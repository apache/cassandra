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

package org.apache.cassandra.cql.common;

public class Utils
{
    /*
     * Strips leading and trailing "'" characters, and handles
     * and escaped characters such as \n, \r, etc.
     * [Shameless clone from hive.]
     */
    public static String unescapeSQLString(String b) 
    {
        assert(b.charAt(0) == '\'');
        assert(b.charAt(b.length()-1) == '\'');
        StringBuilder sb = new StringBuilder(b.length());
        
        for (int i=1; i+1<b.length(); i++)
        {
            if (b.charAt(i) == '\\' && i+2<b.length())
            {
                char n=b.charAt(i+1);
                switch(n)
                {
                case '0': sb.append("\0"); break;
                case '\'': sb.append("'"); break;
                case '"': sb.append("\""); break;
                case 'b': sb.append("\b"); break;
                case 'n': sb.append("\n"); break;
                case 'r': sb.append("\r"); break;
                case 't': sb.append("\t"); break;
                case 'Z': sb.append("\u001A"); break;
                case '\\': sb.append("\\"); break;
                case '%': sb.append("%"); break;
                case '_': sb.append("_"); break;
                default: sb.append(n);
                }
            } 
            else
            {
                sb.append(b.charAt(i));
            }
        }
        return sb.toString();
    } 
}
