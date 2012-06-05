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

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.FloatType;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.LexicalUUIDType;
import org.apache.cassandra.db.marshal.MarshalException;
import org.apache.cassandra.thrift.InvalidRequestException;

/** A term parsed from a CQL statement. */
public class Term implements Value
{
    public enum Type
    {
        STRING, INTEGER, UUID, FLOAT, QMARK;

        static Type forInt(int type)
        {
            if ((type == CqlParser.STRING_LITERAL) || (type == CqlParser.IDENT))
                return STRING;
            else if (type == CqlParser.INTEGER)
                return INTEGER;
            else if (type == CqlParser.UUID)
                return UUID;
            else if (type == CqlParser.FLOAT)
                return FLOAT;
            else if (type == CqlParser.QMARK)
                return QMARK;

            // FIXME: handled scenario that should never occur.
            return null;
        }
    }

    private final String text;
    private final Type type;
    public final int bindIndex;
    public final boolean isToken;

    private Term(String text, Type type, int bindIndex, boolean isToken)
    {
        this.text = text == null ? "" : text;
        this.type = type;
        this.bindIndex = bindIndex;
        this.isToken = isToken;
    }

    public Term(String text, Type type)
    {
        this(text, type, -1, false);
    }

    /**
     * Create new Term instance from a string, and an integer that corresponds
     * with the token ID from CQLParser.
     *
     * @param text the text representation of the term.
     * @param type the term's type as an integer token ID.
     */
    public Term(String text, int type)
    {
        this(text, Type.forInt(type));
    }

    public Term(long value, Type type)
    {
        this(String.valueOf(value), type);
    }

    public Term(String text, int type, int index)
    {
        this(text, Type.forInt(type), index, false);
    }

    public static Term tokenOf(Term t)
    {
        return new Term(t.text, t.type, t.bindIndex, true);
    }

    /**
     * Returns the text parsed to create this term.
     *
     * @return the string term acquired from a CQL statement.
     */
    public String getText()
    {
        return isToken ? "token(" + text + ")" : text;
    }

    /**
     * Returns the typed value, serialized to a ByteBuffer according to a
     * comparator/validator.
     *
     * @return a ByteBuffer of the value.
     * @throws InvalidRequestException if unable to coerce the string to its type.
     */
    public ByteBuffer getByteBuffer(AbstractType<?> validator, List<ByteBuffer> variables) throws InvalidRequestException
    {
        try
        {
            if (!isBindMarker())
                return validator.fromString(text);

            // must be a marker term so check for a CqlBindValue stored in the term
            if (bindIndex == -1)
                throw new AssertionError("a marker Term was encountered with no index value");

            ByteBuffer value = variables.get(bindIndex);
            validator.validate(value);
            return value;
        }
        catch (MarshalException e)
        {
            throw new InvalidRequestException(e.getMessage());
        }
    }

    public Token getAsToken(AbstractType<?> validator, List<ByteBuffer> variables, IPartitioner<?> p) throws InvalidRequestException
    {
        if (!(isToken || type == Type.STRING))
            throw new InvalidRequestException("Invalid value for token (use a string literal of the token value or the token() function)");

        try
        {
            if (isToken)
            {
                ByteBuffer value = getByteBuffer(validator, variables);
                return p.getToken(value);
            }
            else
            {
                p.getTokenFactory().validate(text);
                return p.getTokenFactory().fromString(text);
            }
        }
        catch (ConfigurationException e)
        {
            throw new InvalidRequestException(e.getMessage());
        }
    }

    /**
     * Obtain the term's type.
     *
     * @return the type
     */
    public Type getType()
    {
        return type;
    }

    public boolean isBindMarker()
    {
        return type == Type.QMARK;
    }

    public List<Term> asList()
    {
        return Collections.singletonList(this);
    }

    @Override
    public String toString()
    {
        return String.format("Term(%s, type=%s%s)", getText(), type, isToken ? ", isToken" : "");
    }

    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1 + (isToken ? 1 : 0);
        result = prime * result + ((text == null) ? 0 : text.hashCode());
        result = prime * result + ((type == null) ? 0 : type.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        Term other = (Term) obj;
        if (type==Type.QMARK) return false; // markers are never equal
        if (text == null)
        {
            if (other.text != null)
                return false;
        } else if (!text.equals(other.text))
            return false;
        if (type != other.type)
            return false;
        if (isToken != other.isToken)
            return false;
        return true;
    }
}
