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

import org.apache.cassandra.db.marshal.LexicalUUIDType;
import org.apache.cassandra.db.marshal.TimeUUIDType;
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
                return ByteBuffer.wrap(text.getBytes());
            case LONG:
                try
                {
                    return ByteBufferUtil.bytes(Long.parseLong(text));
                }
                catch (NumberFormatException e)
                {
                    throw new InvalidRequestException(text + " is not valid for type long");
                }
            case INTEGER: 
                try
                {
                    return ByteBufferUtil.bytes(Integer.parseInt(text));
                }
                catch (NumberFormatException e)
                {
                    throw new InvalidRequestException(text + " is not valid for type int");
                }
            case UNICODE:
                try
                {
                    return ByteBuffer.wrap(text.getBytes("UTF-8"));
                }
                catch (UnsupportedEncodingException e)
                {
                   throw new RuntimeException(e);
                }
            case UUID:
                try
                {
                    return LexicalUUIDType.instance.fromString(text);
                }
                catch (IllegalArgumentException e)
                {
                    throw new InvalidRequestException(text + " is not valid for type uuid");
                }
            case TIMEUUID:
                if (text.equals("") || text.toLowerCase().equals("now"))
                {
                    return ByteBuffer.wrap(UUIDGen.decompose(UUIDGen.makeType1UUIDFromHost(FBUtilities.getLocalAddress())));
                }
                
                // Milliseconds since epoch?
                if (text.matches("^\\d+$"))
                {
                    try
                    {
                        return ByteBuffer.wrap(UUIDGen.getTimeUUIDBytes(Long.parseLong(text)));
                    }
                    catch (NumberFormatException e)
                    {
                        throw new InvalidRequestException(text + " is not valid for type timeuuid");
                    }
                }
                
                try
                {
                    long timestamp = DateUtils.parseDate(text, iso8601Patterns).getTime();
                    return ByteBuffer.wrap(UUIDGen.getTimeUUIDBytes(timestamp));
                }
                catch (ParseException e1)
                {
                    // Ignore failures; we'll move onto the Next Thing.
                }
                
                // Last chance, a UUID string (i.e. f79326be-2d7b-11e0-b074-0026c650d722)
                try
                {
                    return TimeUUIDType.instance.fromString(text);
                }
                catch (IllegalArgumentException e)
                {
                    throw new InvalidRequestException(text + " is not valid for type timeuuid");
                }
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
    STRING, LONG, INTEGER, UNICODE, UUID, TIMEUUID;
    
    static TermType forInt(int type)
    {
        if (type == CqlParser.STRING_LITERAL)
            return STRING;
        else if (type == CqlParser.LONG)
            return LONG;
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
