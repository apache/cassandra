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

import java.util.Objects;

import org.antlr.runtime.Token;

import static java.lang.Math.max;
import static java.lang.Math.min;

public class StatementSource
{
    public static final StatementSource INTERNAL = new StatementSource(0, 0);

    public final int line;
    public final int charPositionInLine;

    public StatementSource(int line, int charPositionInLine)
    {
        this.line = line;
        this.charPositionInLine = charPositionInLine;
    }

    @Override
    public String toString()
    {
        if (this == INTERNAL)
        {
            return "<<<internal statement>>>";
        }
        else
        {
            if (!isEmpty())
                return String.format("at [%d:%d]", line + 1, charPositionInLine + 1);
            else
                return "";
        }
    }

    public boolean isEmpty()
    {
        return line > Character.MAX_VALUE || line == Character.MAX_VALUE && charPositionInLine > Character.MAX_VALUE;
    }

    // note - this can also reproduce the original statement raw text by getting TokenStream and calling toString(startToken, endToken)
    public static StatementSource create(Token startToken)
    {
        Objects.requireNonNull(startToken);

        if (startToken.getType() == Token.EOF)
            return new StatementSource(Character.MAX_VALUE + 1, 0);

        int startLine = min(max(startToken.getLine(), 1) - 1, Character.MAX_VALUE);
        int startChar = min(max(startToken.getCharPositionInLine(), 0), Character.MAX_VALUE);

        return new StatementSource(startLine, startChar);
    }

}
