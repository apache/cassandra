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

package org.apache.cassandra.cli;

import org.antlr.runtime.*;
import org.antlr.runtime.tree.*;
import org.apache.cassandra.cql.common.Utils;


public class CliCompiler
{

    // ANTLR does not provide case-insensitive tokenization support
    // out of the box. So we override the LA (lookahead) function
    // of the ANTLRStringStream class. Note: This doesn't change the
    // token text-- but just relaxes the matching rules to match
    // in upper case. [Logic borrowed from Hive code.]
    // 
    // Also see discussion on this topic in:
    // http://www.antlr.org/wiki/pages/viewpage.action?pageId=1782.
    public static class ANTLRNoCaseStringStream  extends ANTLRStringStream
    {
        public ANTLRNoCaseStringStream(String input)
        {
            super(input);
        }
    
        public int LA(int i)
        {
            int returnChar = super.LA(i);
            if (returnChar == CharStream.EOF)
            {
                return returnChar; 
            }
            else if (returnChar == 0) 
            {
                return returnChar;
            }

            return Character.toUpperCase((char)returnChar);
        }
    }

    public static CommonTree compileQuery(String query)
    {
        CommonTree queryTree = null;
        try
        {
            ANTLRStringStream input = new ANTLRNoCaseStringStream(query);

            CliLexer lexer = new CliLexer(input);
            CommonTokenStream tokens = new CommonTokenStream(lexer);

            CliParser parser = new CliParser(tokens);

            // start parsing...
            queryTree = (CommonTree)(parser.root().getTree());

            // semantic analysis if any...
            //  [tbd]

        }
        catch(Exception e)
        {
            System.err.println("Exception " + e.getMessage());
            e.printStackTrace(System.err);
        }
        return queryTree;
    }
    /*
     * NODE_COLUMN_ACCESS related functions.
     */
    public static String getTableName(CommonTree astNode)
    {
        assert(astNode.getType() == CliParser.NODE_COLUMN_ACCESS);

        return astNode.getChild(0).getText();
    }

    public static String getColumnFamily(CommonTree astNode)
    {
        assert(astNode.getType() == CliParser.NODE_COLUMN_ACCESS);

        return astNode.getChild(1).getText();
    }

    public static String getKey(CommonTree astNode)
    {
        assert(astNode.getType() == CliParser.NODE_COLUMN_ACCESS);

        return Utils.unescapeSQLString(astNode.getChild(2).getText());
    }

    public static int numColumnSpecifiers(CommonTree astNode)
    {
        // Skip over table, column family and rowKey
        return astNode.getChildCount() - 3;
    }

    // Returns the pos'th (0-based index) column specifier in the astNode
    public static String getColumn(CommonTree astNode, int pos)
    {
        // Skip over table, column family and rowKey
        return Utils.unescapeSQLString(astNode.getChild(pos + 3).getText()); 
    }
 
}
