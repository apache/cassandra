/**
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
 */
package org.apache.cassandra.io.compress;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;

import org.apache.cassandra.config.ConfigurationException;

public class CompressionParameters
{
    public final static int DEFAULT_CHUNK_LENGTH = 65536;
    public static final String CHUNK_LENGTH_PARAMETER = "chunk_length_kb";

    public final Class<? extends ICompressor> compressorClass;
    public final Map<String, String> compressionOptions;

    public final transient ICompressor compressor;
    public final transient int chunkLength;

    public CompressionParameters(CharSequence compressorClassName, Map<? extends CharSequence, ? extends CharSequence> options) throws ConfigurationException
    {
        this(compressorClassName, copyOptions(options), -1);
    }

    public CompressionParameters(CharSequence compressorClassName, Map<String, String> options, int chunkLength) throws ConfigurationException
    {
        this(createCompressor(parseCompressorClass(compressorClassName), options), options, chunkLength < 0 ? getChunkLength(options) : chunkLength);
        validateChunkLength();
    }

    public CompressionParameters(ICompressor compressor)
    {
        this(compressor, null, DEFAULT_CHUNK_LENGTH);
    }

    public CompressionParameters(ICompressor compressor, Map<String, String> compressionOptions, int chunkLength)
    {
        this.compressorClass = compressor == null ? null : compressor.getClass();
        this.compressionOptions = compressor == null ? null : (compressionOptions == null ? Collections.<String, String>emptyMap() : compressionOptions);
        this.chunkLength = chunkLength;
        this.compressor = compressor;
    }

    private static Class<? extends ICompressor> parseCompressorClass(CharSequence cc) throws ConfigurationException
    {
        if (cc == null)
            return null;

        String className = cc.toString();
        className = className.contains(".") ? className : "org.apache.cassandra.io.compress." + className;
        try
        {
            return (Class<? extends ICompressor>)Class.forName(className);
        }
        catch (Exception e)
        {
            throw new ConfigurationException("Could not create Compression for type " + cc.toString(), e);
        }
    }

    private static ICompressor createCompressor(Class<? extends ICompressor> compressorClass, Map<String, String> compressionOptions) throws ConfigurationException
    {
        if (compressorClass == null)
            return null;

        try
        {
            Method method = compressorClass.getMethod("create", Map.class);
            return (ICompressor)method.invoke(null, compressionOptions);
        }
        catch (NoSuchMethodException e)
        {
            throw new ConfigurationException("create method not found", e);
        }
        catch (SecurityException e)
        {
            throw new ConfigurationException("Access forbiden", e);
        }
        catch (IllegalAccessException e)
        {
            throw new ConfigurationException("Cannot access method create in " + compressorClass.getName(), e);
        }
        catch (InvocationTargetException e)
        {
            throw new ConfigurationException(compressorClass.getSimpleName() + ".create() throwed an error", e);
        }
        catch (ExceptionInInitializerError e)
        {
            throw new ConfigurationException("Cannot initialize class " + compressorClass.getName());
        }
    }

    private static Map<String, String> copyOptions(Map<? extends CharSequence, ? extends CharSequence> co)
    {
        if (co == null || co.isEmpty())
            return Collections.<String, String>emptyMap();

        Map<String, String> compressionOptions = new HashMap<String, String>();
        for (Map.Entry<? extends CharSequence, ? extends CharSequence> entry : co.entrySet())
        {
            compressionOptions.put(entry.getKey().toString(), entry.getValue().toString());
        }
        return compressionOptions;
    }

    private static int getChunkLength(Map<String, String> options) throws ConfigurationException
    {
        int chunkLength = DEFAULT_CHUNK_LENGTH;
        if (options != null && options.containsKey(CHUNK_LENGTH_PARAMETER))
        {
            try
            {
                chunkLength = Integer.parseInt(options.get(CHUNK_LENGTH_PARAMETER));
            }
            catch (NumberFormatException e)
            {
                throw new ConfigurationException("Invalid value for " + CHUNK_LENGTH_PARAMETER, e);
            }
        }
        return chunkLength;
    }

    // chunkLength must be a power of 2 because we assume so when
    // computing the chunk number from an uncompressed file offset (see
    // CompressedRandomAccessReader.decompresseChunk())
    private void validateChunkLength() throws ConfigurationException
    {
        if (chunkLength <= 0)
            throw new ConfigurationException("Invalid negative or null " + CHUNK_LENGTH_PARAMETER);

        int c = chunkLength;
        boolean found = false;
        while (c != 0)
        {
            if ((c & 0x01) != 0)
            {
                if (found)
                    throw new ConfigurationException(CHUNK_LENGTH_PARAMETER + " must be a power of 2");
                else
                    found = true;
            }
            c >>= 1;
        }
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj == this)
        {
            return true;
        }
        else if (obj == null || obj.getClass() != getClass())
        {
            return false;
        }

        CompressionParameters cp = (CompressionParameters) obj;
        return new EqualsBuilder()
            .append(compressorClass, cp.compressorClass)
            .append(compressionOptions, cp.compressionOptions)
            .isEquals();
    }

    @Override
    public int hashCode()
    {
        return new HashCodeBuilder(29, 1597)
            .append(compressorClass)
            .append(compressionOptions)
            .toHashCode();
    }
}
