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

import org.apache.avro.util.Utf8;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;

import org.apache.cassandra.config.ConfigurationException;

public class CompressionParameters
{
    public final static int DEFAULT_CHUNK_LENGTH = 65536;

    public static final String SSTABLE_COMPRESSION = "sstable_compression";
    public static final String CHUNK_LENGTH = "chunk_length_kb";

    public final ICompressor sstableCompressor;
    private final Integer chunkLength;
    public final Map<String, String> otherOptions; // Unrecognized options, can be use by the compressor

    public static CompressionParameters create(Map<? extends CharSequence, ? extends CharSequence> opts) throws ConfigurationException
    {
        Map<String, String> options = copyOptions(opts);
        String sstableCompressionClass = options.get(SSTABLE_COMPRESSION);
        String chunkLength = options.get(CHUNK_LENGTH);
        options.remove(SSTABLE_COMPRESSION);
        options.remove(CHUNK_LENGTH);
        CompressionParameters cp = new CompressionParameters(sstableCompressionClass, parseChunkLength(chunkLength), options);
        cp.validateChunkLength();
        return cp;
    }

    public CompressionParameters(String sstableCompressorClass, Integer chunkLength, Map<String, String> otherOptions) throws ConfigurationException
    {
        this(createCompressor(parseCompressorClass(sstableCompressorClass), otherOptions), chunkLength, otherOptions);
    }

    public CompressionParameters(ICompressor sstableCompressor)
    {
        this(sstableCompressor, null, Collections.<String, String>emptyMap());
    }

    public CompressionParameters(ICompressor sstableCompressor, Integer chunkLength, Map<String, String> otherOptions)
    {
        this.sstableCompressor = sstableCompressor;
        this.chunkLength = chunkLength;
        this.otherOptions = otherOptions;
    }

    public int chunkLength()
    {
        return chunkLength == null ? DEFAULT_CHUNK_LENGTH : chunkLength;
    }


    private static Class<? extends ICompressor> parseCompressorClass(String className) throws ConfigurationException
    {
        if (className == null)
            return null;

        className = className.contains(".") ? className : "org.apache.cassandra.io.compress." + className;
        try
        {
            return (Class<? extends ICompressor>)Class.forName(className);
        }
        catch (Exception e)
        {
            throw new ConfigurationException("Could not create Compression for type " + className, e);
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

    private static Integer parseChunkLength(String chLength) throws ConfigurationException
    {
        if (chLength == null)
            return null;

        try
        {
            return Integer.parseInt(chLength);
        }
        catch (NumberFormatException e)
        {
            throw new ConfigurationException("Invalid value for " + CHUNK_LENGTH, e);
        }
    }

    // chunkLength must be a power of 2 because we assume so when
    // computing the chunk number from an uncompressed file offset (see
    // CompressedRandomAccessReader.decompresseChunk())
    private void validateChunkLength() throws ConfigurationException
    {
        if (chunkLength == null)
            return; // chunk length not set, this is fine, default will be used

        if (chunkLength <= 0)
            throw new ConfigurationException("Invalid negative or null " + CHUNK_LENGTH);

        int c = chunkLength;
        boolean found = false;
        while (c != 0)
        {
            if ((c & 0x01) != 0)
            {
                if (found)
                    throw new ConfigurationException(CHUNK_LENGTH + " must be a power of 2");
                else
                    found = true;
            }
            c >>= 1;
        }
    }

    public Map<CharSequence, CharSequence> asAvroOptions()
    {
        Map<CharSequence, CharSequence> options = new HashMap<CharSequence, CharSequence>();
        for (Map.Entry<String, String> entry : otherOptions.entrySet())
            options.put(new Utf8(entry.getKey()), new Utf8(entry.getValue()));

        if (sstableCompressor == null)
            return options;

        options.put(new Utf8(SSTABLE_COMPRESSION), new Utf8(sstableCompressor.getClass().getName()));
        if (chunkLength != null)
            options.put(new Utf8(CHUNK_LENGTH), new Utf8(chunkLength.toString()));
        return options;
    }

    public Map<String, String> asThriftOptions()
    {
        Map<String, String> options = new HashMap<String, String>(otherOptions);
        if (sstableCompressor == null)
            return options;

        options.put(SSTABLE_COMPRESSION, sstableCompressor.getClass().getName());
        if (chunkLength != null)
            options.put(CHUNK_LENGTH, chunkLength.toString());
        return options;
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
            .append(sstableCompressor, cp.sstableCompressor)
            .append(chunkLength, cp.chunkLength)
            .append(otherOptions, cp.otherOptions)
            .isEquals();
    }

    @Override
    public int hashCode()
    {
        return new HashCodeBuilder(29, 1597)
            .append(sstableCompressor)
            .append(chunkLength)
            .append(otherOptions)
            .toHashCode();
    }
}
