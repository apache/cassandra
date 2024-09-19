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

import org.junit.Test;

import org.antlr.runtime.Token;
import org.mockito.Mockito;

import static org.apache.cassandra.cql3.StatementSource.create;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

public class StatementSourceTest
{
    private static Token token(int line, int pos)
    {
        Token token = Mockito.mock(Token.class);
        when(token.getLine()).thenReturn(line);
        when(token.getCharPositionInLine()).thenReturn(pos);
        when(token.getType()).thenReturn(1);
        return token;
    }

    private static Token eof()
    {
        Token token = Mockito.mock(Token.class);
        when(token.getLine()).thenThrow(UnsupportedOperationException.class);
        when(token.getCharPositionInLine()).thenThrow(UnsupportedOperationException.class);
        when(token.getType()).thenReturn(Token.EOF);
        return token;
    }

    @Test
    public void test()
    {
        assertThat(create(token(1, 4))).hasToString("at [1:5]");
        assertThat(create(token(3, 8))).hasToString("at [3:9]");
        assertThat(create(token(6, 8))).hasToString("at [6:9]");
        assertThat(create(token(1, 0))).hasToString("at [1:1]");
        assertThat(create(eof()).toString()).isEmpty();

        assertThat(StatementSource.INTERNAL).hasToString("<<<internal statement>>>");
    }
}
