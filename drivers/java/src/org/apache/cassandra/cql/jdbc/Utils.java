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

package org.apache.cassandra.cql.jdbc;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.sql.Types;
import java.util.UUID;
import java.util.zip.Deflater;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.thrift.Compression;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class Utils
{
    private static final Logger logger = LoggerFactory.getLogger(Utils.class);
    
    public static ByteBuffer compressQuery(String queryStr, Compression compression)
    {
        byte[] data = queryStr.getBytes();
        Deflater compressor = new Deflater();
        compressor.setInput(data);
        compressor.finish();
        
        ByteArrayOutputStream byteArray = new ByteArrayOutputStream();
        byte[] buffer = new byte[1024];
        
        while (!compressor.finished())
        {
            int size = compressor.deflate(buffer);
            byteArray.write(buffer, 0, size);
        }
        
        logger.trace("Compressed query statement {} bytes in length to {} bytes",
                     data.length,
                     byteArray.size());
        
        return ByteBuffer.wrap(byteArray.toByteArray());
    }
    
    static int getJdbcType(AbstractType type) throws SQLException
    {   
        if (type instanceof ColumnMetaData)
            return ((ColumnMetaData)type).getType();
        else if (type == IntegerType.instance)
            return Types.BIGINT;
        else if (type.getType().equals(Long.class))
            return Types.BIGINT; // not the best fit.
        else if (type.getType().equals(String.class))
            return Types.VARCHAR;
        else if (type.getType().equals(UUID.class))
            return Types.TIMESTAMP;
        else if (type == BytesType.instance)
            return Types.BINARY;
        else
            throw new SQLException("Uninterpretable JDBC type " + type.getClass().getName());
    }
    
    static boolean isTypeSigned(AbstractType type)
    {
        if (type == IntegerType.instance || type == LongType.instance)
            return true;
        else if (type instanceof ColumnMetaData) 
            return ((ColumnMetaData)type).isSigned();
        else
            return false;
    }
    
    static int getTypeScale(AbstractType type) 
    {
        if (type instanceof ColumnMetaData)
            return ((ColumnMetaData)type).getScale();
        else
            return 0;
    }
}
