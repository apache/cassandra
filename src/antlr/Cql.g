/*
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
 */

grammar Cql;

options {
    language = Java;
}

import Parser,Lexer;

@header {
    package org.apache.cassandra.cql3;

    import java.util.ArrayList;
    import java.util.Arrays;
    import java.util.Collections;
    import java.util.EnumSet;
    import java.util.HashSet;
    import java.util.HashMap;
    import java.util.LinkedHashMap;
    import java.util.List;
    import java.util.Map;
    import java.util.Set;

    import org.apache.cassandra.auth.*;
    import org.apache.cassandra.cql3.*;
    import org.apache.cassandra.cql3.restrictions.CustomIndexExpression;
    import org.apache.cassandra.cql3.statements.*;
    import org.apache.cassandra.cql3.selection.*;
    import org.apache.cassandra.cql3.functions.*;
    import org.apache.cassandra.db.marshal.CollectionType;
    import org.apache.cassandra.exceptions.ConfigurationException;
    import org.apache.cassandra.exceptions.InvalidRequestException;
    import org.apache.cassandra.exceptions.SyntaxException;
    import org.apache.cassandra.utils.Pair;
}

@members {
    public void addErrorListener(ErrorListener listener)
    {
        gParser.addErrorListener(listener);
    }

    public void removeErrorListener(ErrorListener listener)
    {
        gParser.removeErrorListener(listener);
    }

    public void displayRecognitionError(String[] tokenNames, RecognitionException e)
    {
        gParser.displayRecognitionError(tokenNames, e);
    }

    protected void addRecognitionError(String msg)
    {
        gParser.addRecognitionError(msg);
    }
}

@lexer::header {
    package org.apache.cassandra.cql3;

    import org.apache.cassandra.exceptions.SyntaxException;
}

@lexer::members {
    List<Token> tokens = new ArrayList<Token>();

    public void emit(Token token)
    {
        state.token = token;
        tokens.add(token);
    }

    public Token nextToken()
    {
        super.nextToken();
        if (tokens.size() == 0)
            return new CommonToken(Token.EOF);
        return tokens.remove(0);
    }

    private final List<ErrorListener> listeners = new ArrayList<ErrorListener>();

    public void addErrorListener(ErrorListener listener)
    {
        this.listeners.add(listener);
    }

    public void removeErrorListener(ErrorListener listener)
    {
        this.listeners.remove(listener);
    }

    public void displayRecognitionError(String[] tokenNames, RecognitionException e)
    {
        for (int i = 0, m = listeners.size(); i < m; i++)
            listeners.get(i).syntaxError(this, tokenNames, e);
    }
}

query returns [ParsedStatement stmnt]
    : st=cqlStatement (';')* EOF { $stmnt = st; }
    ;
