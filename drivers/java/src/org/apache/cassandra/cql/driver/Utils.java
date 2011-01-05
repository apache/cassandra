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

package org.apache.cassandra.cql.driver;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.zip.Deflater;

import org.apache.cassandra.thrift.Compression;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Utils
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
}
