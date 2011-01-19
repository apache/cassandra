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

import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

/**
 * A term parsed from a CQL statement.
 *
 */
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
        this.text = text;
        this.type = TermType.forInt(type);
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
     */
    public ByteBuffer getByteBuffer()
    {
        switch (type)
        {
            case STRING:
                return ByteBuffer.wrap(text.getBytes());
            case LONG:
                return ByteBufferUtil.bytes(Long.parseLong(text));
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
    STRING, LONG;
    
    static TermType forInt(int type)
    {
        if (type == CqlParser.STRING_LITERAL)
            return STRING;
        else if (type == CqlParser.LONG)
            return LONG;
        
        // FIXME: handled scenario that should never occur.
        return null;
    }
}
