/*
 * 
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
 * 
 */
package org.apache.cassandra.cql;

import java.nio.ByteBuffer;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.FloatType;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.LexicalUUIDType;
import org.apache.cassandra.db.marshal.MarshalException;
import org.apache.cassandra.thrift.InvalidRequestException;

/** A term parsed from a CQL statement. */
public class Term
{   
    private final String text;
    private final TermType type;
    
    /**
     * Create new Term instance from a string, and an integer that corresponds
     * with the token ID from CQLParser.
     * 
     * @param text the text representation of the term.
     * @param type the term's type as an integer token ID.
     */
    public Term(String text, int type)
    {
        this.text = text == null ? "" : text;
        this.type = TermType.forInt(type);
    }
    
    public Term(String text, TermType type)
    {
        this.text = text == null ? "" : text;
        this.type = type;
    }

    public Term(long value, TermType type)
    {
        this.text = String.valueOf(value);
        this.type = type;
    }
    
    protected Term()
    {
        this.text = "";
        this.type = TermType.STRING;
    }

    /**
     * Returns the text parsed to create this term.
     * 
     * @return the string term acquired from a CQL statement.
     */
    public String getText()
    {
        return text;
    }
    
    /**
     * Returns the typed value, serialized to a ByteBuffer according to a
     * comparator/validator.
     * 
     * @return a ByteBuffer of the value.
     * @throws InvalidRequestException if unable to coerce the string to its type.
     */
    public ByteBuffer getByteBuffer(AbstractType<?> validator) throws InvalidRequestException
    {
        try
        {
            return validator.fromString(text);
        }
        catch (MarshalException e)
        {
            throw new InvalidRequestException(e.getMessage());
        }
    }
    
    /**
     * Returns the typed value, serialized to a ByteBuffer.
     * 
     * @return a ByteBuffer of the value.
     * @throws InvalidRequestException if unable to coerce the string to its type.
     */
    public ByteBuffer getByteBuffer() throws InvalidRequestException
    {
        switch (type)
        {
            case STRING:
                return AsciiType.instance.fromString(text);
            case INTEGER: 
                return IntegerType.instance.fromString(text);
            case UUID:
                // we specifically want the Lexical class here, not "UUIDType," because we're supposed to have
                // a uuid-shaped string here, and UUIDType also accepts integer or date strings (and turns them into version 1 uuids).
                return LexicalUUIDType.instance.fromString(text);
            case FLOAT: 
              return FloatType.instance.fromString(text);
        }
        
        // FIXME: handle scenario that should never happen
        return null;
    }

    /**
     * Obtain the term's type.
     * 
     * @return the type
     */
    public TermType getType()
    {
        return type;
    }
    
    public String toString()
    {
        return String.format("Term(%s, type=%s)", getText(), type);
    }

    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
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
        if (text == null)
        {
            if (other.text != null)
                return false;
        } else if (!text.equals(other.text))
            return false;
        if (type != other.type)
            return false;
        return true;
    }

    
}

enum TermType
{
    STRING, INTEGER, UUID, FLOAT;
    
    static TermType forInt(int type)
    {
        if ((type == CqlParser.STRING_LITERAL) || (type == CqlParser.IDENT))
            return STRING;
        else if (type == CqlParser.INTEGER)
            return INTEGER;
        else if (type == CqlParser.UUID)
          return UUID;
        else if (type == CqlParser.FLOAT)
          return FLOAT;
        
        // FIXME: handled scenario that should never occur.
        return null;
    }
}
