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
package org.apache.cassandra.dht;

abstract class ComparableObjectToken<C extends Comparable<C>> extends Token
{
    private static final long serialVersionUID = 1L;

    final C token;   // Package-private to allow access from subtypes, which should all reside in the dht package.

    protected ComparableObjectToken(C token)
    {
        this.token = token;
    }

    @Override
    public C getTokenValue()
    {
        return token;
    }

    @Override
    public String toString()
    {
        return token.toString();
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj)
            return true;
        if (obj == null || this.getClass() != obj.getClass())
            return false;

        return token.equals(((ComparableObjectToken<?>)obj).token);
    }

    @Override
    public int hashCode()
    {
        return token.hashCode();
    }

    @Override
    @SuppressWarnings("unchecked")
    public int compareTo(Token o)
    {
        if (o.getClass() != getClass())
            throw new IllegalArgumentException("Invalid type of Token.compareTo() argument.");

        return token.compareTo(((ComparableObjectToken<C>) o).token);
    }

    @Override
    public double size(Token next)
    {
        throw new UnsupportedOperationException(String.format("Token type %s does not support token allocation.",
                                                              getClass().getSimpleName()));
    }

    @Override
    public Token increaseSlightly()
    {
        throw new UnsupportedOperationException(String.format("Token type %s does not support token allocation.",
                                                              getClass().getSimpleName()));
    }
}
