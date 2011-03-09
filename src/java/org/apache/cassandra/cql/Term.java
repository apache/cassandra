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

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.text.ParseException;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.LexicalUUIDType;
import org.apache.cassandra.db.marshal.MarshalException;
import org.apache.cassandra.db.marshal.TimeUUIDType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.UUIDGen;
import org.apache.commons.lang.time.DateUtils;

/** A term parsed from a CQL statement. */
public class Term
{
    private static String[] iso8601Patterns = new String[] {
            "yyyy-MM-dd HH:mm",
            "yyyy-MM-dd HH:mm:ss",
            "yyyy-MM-dd HH:mmZ",
            "yyyy-MM-dd HH:mm:ssZ",
            "yyyy-MM-dd'T'HH:mm",
            "yyyy-MM-dd'T'HH:mmZ",
            "yyyy-MM-dd'T'HH:mm:ss",
            "yyyy-MM-dd'T'HH:mm:ssZ",
            "yyyy-MM-dd",
            "yyyy-MM-ddZ"
    };
    
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
            case UNICODE:
                return UTF8Type.instance.fromString(text);
            case UUID:
                return LexicalUUIDType.instance.fromString(text);
            case TIMEUUID:
                return TimeUUIDType.instance.fromString(text);
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
    
}

enum TermType
{
    STRING, INTEGER, UNICODE, UUID, TIMEUUID;
    
    static TermType forInt(int type)
    {
        if (type == CqlParser.STRING_LITERAL)
            return STRING;
        else if (type == CqlParser.INTEGER)
            return INTEGER;
        else if (type == CqlParser.UNICODE)
            return UNICODE;
        else if (type == CqlParser.UUID)
            return UUID;
        
        // FIXME: handled scenario that should never occur.
        return null;
    }
}
