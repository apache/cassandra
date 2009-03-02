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

package org.apache.cassandra.cql.compiler.common;


import org.apache.cassandra.cql.common.*;
import org.apache.cassandra.cql.compiler.parse.*;
import org.apache.cassandra.cql.compiler.sem.*;

import java.util.ArrayList;

import org.antlr.runtime.*;
import org.antlr.runtime.tree.*;
import org.apache.cassandra.cql.common.Plan;
import org.apache.cassandra.cql.compiler.parse.CqlLexer;
import org.apache.cassandra.cql.compiler.parse.CqlParser;
import org.apache.cassandra.cql.compiler.parse.ParseError;
import org.apache.cassandra.cql.compiler.parse.ParseException;
import org.apache.cassandra.cql.compiler.sem.SemanticException;
import org.apache.cassandra.cql.compiler.sem.SemanticPhase;

public class CqlCompiler
{
    // ANTLR does not provide case-insensitive tokenization support
    // out of the box. So we override the LA (lookahead) function
    // of the ANTLRStringStream class. Note: This doesn't change the
    // token text-- but just relaxes the matching rules to match
    // in upper case. [Logic borrowed from Hive code.]
    // 
    // Also see discussion on this topic in:
    // http://www.antlr.org/wiki/pages/viewpage.action?pageId=1782.
    public class ANTLRNoCaseStringStream  extends ANTLRStringStream
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

    // Override CQLParser. This gives flexibility in altering default error
    // messages as well as accumulating multiple errors.
    public class CqlParserX extends CqlParser
    {
        private ArrayList<ParseError> errors;

        public CqlParserX(TokenStream input)
        {
            super(input);
            errors = new ArrayList<ParseError>();
        }

        protected void mismatch(IntStream input, int ttype, BitSet follow) throws RecognitionException
        {
            throw new MismatchedTokenException(ttype, input);
        }

        public void recoverFromMismatchedSet(IntStream input,
                                             RecognitionException re,
                                             BitSet follow) throws RecognitionException
        {
            throw re;
        }

        public void displayRecognitionError(String[] tokenNames,
                                            RecognitionException e)
        {
            errors.add(new ParseError(this, e, tokenNames));
        }

        public ArrayList<ParseError> getErrors()
        {
            return errors;
        }
    }

    // Compile a CQL query
    public Plan compileQuery(String query) throws ParseException, SemanticException
    {
        CommonTree queryTree = null;
        CqlLexer lexer = null;
        CqlParserX parser = null;
        CommonTokenStream tokens = null;

        ANTLRStringStream input = new ANTLRNoCaseStringStream(query);

        lexer = new CqlLexer(input);
        tokens = new CommonTokenStream(lexer);
        parser = new CqlParserX(tokens);

        // built AST
        try
        {
            queryTree = (CommonTree)(parser.root().getTree());
        }
        catch (RecognitionException e)
        {
            throw new ParseException(parser.getErrors());            
        }
        catch (RewriteEmptyStreamException e)
        {
            throw new ParseException(parser.getErrors());            
        }

        if (!parser.getErrors().isEmpty())
        {
            throw new ParseException(parser.getErrors());
        }

        if (!parser.errors.isEmpty())
        {
            throw new ParseException("parser error");
        }

        // Semantic analysis and code-gen.
        // Eventually, I anticipate, I'll be forking these off into two separate phases.
        return SemanticPhase.doSemanticAnalysis(queryTree);
    }
}
