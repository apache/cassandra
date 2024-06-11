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

package org.apache.cassandra.cql3;

import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.CharStream;
import org.antlr.runtime.CommonTokenStream;
import org.antlr.runtime.RecognitionException;
import org.antlr.runtime.TokenStream;
import org.apache.cassandra.exceptions.SyntaxException;

/**
 * Helper class to encapsulate common code that calls one of the generated methods in {@code CqlParser}.
 */
public final class CQLFragmentParser
{

    @FunctionalInterface
    public interface CQLParserFunction<R>
    {
        R parse(CqlParser parser) throws RecognitionException;
    }

    public static <R> R parseAny(CQLParserFunction<R> parserFunction, String input, String meaning)
    {
        try
        {
            return parseAnyUnhandled(parserFunction, input);
        }
        catch (RuntimeException re)
        {
            throw new SyntaxException(String.format("Failed parsing %s: [%s] reason: %s %s",
                                                    meaning,
                                                    input,
                                                    re.getClass().getSimpleName(),
                                                    re.getMessage()));
        }
        catch (RecognitionException e)
        {
            throw new SyntaxException("Invalid or malformed " + meaning + ": " + e.getMessage());
        }
    }

    /**
     * Just call a parser method in {@link CqlParser} - does not do any error handling.
     */
    public static <R> R parseAnyUnhandled(CQLParserFunction<R> parserFunction, String input) throws RecognitionException
    {
        // Lexer and parser
        ErrorCollector errorCollector = new ErrorCollector(input);
        CharStream stream = new ANTLRStringStream(input);
        CqlLexer lexer = new CqlLexer(stream);
        lexer.addErrorListener(errorCollector);

        TokenStream tokenStream = new CommonTokenStream(lexer);
        CqlParser parser = new CqlParser(tokenStream);
        parser.addErrorListener(errorCollector);

        // Parse the query string to a statement instance
        R r = parserFunction.parse(parser);

        // The errorCollector has queue up any errors that the lexer and parser may have encountered
        // along the way, if necessary, we turn the last error into exceptions here.
        errorCollector.throwFirstSyntaxError();

        return r;
    }
}
