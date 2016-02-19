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
package org.apache.cassandra.schema;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.ParameterizedClass;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.compress.*;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;

import static java.lang.String.format;

@SuppressWarnings("deprecation")
public final class CompressionParams
{
    private static final Logger logger = LoggerFactory.getLogger(CompressionParams.class);

    private static volatile boolean hasLoggedSsTableCompressionWarning;
    private static volatile boolean hasLoggedChunkLengthWarning;
    private static volatile boolean hasLoggedCrcCheckChanceWarning;

    public static final int DEFAULT_CHUNK_LENGTH = 65536;
    public static final IVersionedSerializer<CompressionParams> serializer = new Serializer();

    public static final String CLASS = "class";
    public static final String CHUNK_LENGTH_IN_KB = "chunk_length_in_kb";
    public static final String ENABLED = "enabled";

    public static final CompressionParams DEFAULT = new CompressionParams(LZ4Compressor.create(Collections.<String, String>emptyMap()),
                                                                          DEFAULT_CHUNK_LENGTH,
                                                                          Collections.emptyMap());

    private static final String CRC_CHECK_CHANCE_WARNING = "The option crc_check_chance was deprecated as a compression option. " +
                                                           "You should specify it as a top-level table option instead";

    @Deprecated public static final String SSTABLE_COMPRESSION = "sstable_compression";
    @Deprecated public static final String CHUNK_LENGTH_KB = "chunk_length_kb";
    @Deprecated public static final String CRC_CHECK_CHANCE = "crc_check_chance";

    private final ICompressor sstableCompressor;
    private final Integer chunkLength;
    private final ImmutableMap<String, String> otherOptions; // Unrecognized options, can be used by the compressor

    private volatile double crcCheckChance = 1.0;

    public static CompressionParams fromMap(Map<String, String> opts)
    {
        Map<String, String> options = copyOptions(opts);

        String sstableCompressionClass;

        if (!opts.isEmpty() && isEnabled(opts) && !containsSstableCompressionClass(opts))
            throw new ConfigurationException(format("Missing sub-option '%s' for the 'compression' option.", CLASS));

        if (!removeEnabled(options))
        {
            sstableCompressionClass = null;

            if (!options.isEmpty())
                throw new ConfigurationException(format("If the '%s' option is set to false no other options must be specified", ENABLED));
        }
        else
        {
            sstableCompressionClass = removeSstableCompressionClass(options);
        }

        Integer chunkLength = removeChunkLength(options);

        CompressionParams cp = new CompressionParams(sstableCompressionClass, chunkLength, options);
        cp.validate();

        return cp;
    }

    public Class<? extends ICompressor> klass()
    {
        return sstableCompressor.getClass();
    }

    public static CompressionParams noCompression()
    {
        return new CompressionParams((ICompressor) null, DEFAULT_CHUNK_LENGTH, Collections.emptyMap());
    }

    public static CompressionParams snappy()
    {
        return snappy(null);
    }

    public static CompressionParams snappy(Integer chunkLength)
    {
        return new CompressionParams(SnappyCompressor.instance, chunkLength, Collections.emptyMap());
    }

    public static CompressionParams deflate()
    {
        return deflate(null);
    }

    public static CompressionParams deflate(Integer chunkLength)
    {
        return new CompressionParams(DeflateCompressor.instance, chunkLength, Collections.emptyMap());
    }

    public static CompressionParams lz4()
    {
        return lz4(null);
    }

    public static CompressionParams lz4(Integer chunkLength)
    {
        return new CompressionParams(LZ4Compressor.create(Collections.emptyMap()), chunkLength, Collections.emptyMap());
    }

    public CompressionParams(String sstableCompressorClass, Integer chunkLength, Map<String, String> otherOptions) throws ConfigurationException
    {
        this(createCompressor(parseCompressorClass(sstableCompressorClass), otherOptions), chunkLength, otherOptions);
    }

    private CompressionParams(ICompressor sstableCompressor, Integer chunkLength, Map<String, String> otherOptions) throws ConfigurationException
    {
        this.sstableCompressor = sstableCompressor;
        this.chunkLength = chunkLength;
        this.otherOptions = ImmutableMap.copyOf(otherOptions);
    }

    public CompressionParams copy()
    {
        return new CompressionParams(sstableCompressor, chunkLength, otherOptions);
    }

    /**
     * Checks if compression is enabled.
     * @return {@code true} if compression is enabled, {@code false} otherwise.
     */
    public boolean isEnabled()
    {
        return sstableCompressor != null;
    }

    /**
     * Returns the SSTable compressor.
     * @return the SSTable compressor or {@code null} if compression is disabled.
     */
    public ICompressor getSstableCompressor()
    {
        return sstableCompressor;
    }

    public ImmutableMap<String, String> getOtherOptions()
    {
        return otherOptions;
    }

    public int chunkLength()
    {
        return chunkLength == null ? DEFAULT_CHUNK_LENGTH : chunkLength;
    }

    private static Class<?> parseCompressorClass(String className) throws ConfigurationException
    {
        if (className == null || className.isEmpty())
            return null;

        className = className.contains(".") ? className : "org.apache.cassandra.io.compress." + className;
        try
        {
            return Class.forName(className);
        }
        catch (Exception e)
        {
            throw new ConfigurationException("Could not create Compression for type " + className, e);
        }
    }

    private static ICompressor createCompressor(Class<?> compressorClass, Map<String, String> compressionOptions) throws ConfigurationException
    {
        if (compressorClass == null)
        {
            if (!compressionOptions.isEmpty())
                throw new ConfigurationException("Unknown compression options (" + compressionOptions.keySet() + ") since no compression class found");
            return null;
        }

        if (compressionOptions.containsKey(CRC_CHECK_CHANCE))
        {
            if (!hasLoggedCrcCheckChanceWarning)
            {
                logger.warn(CRC_CHECK_CHANCE_WARNING);
                hasLoggedCrcCheckChanceWarning = true;
            }
            compressionOptions.remove(CRC_CHECK_CHANCE);
        }

        try
        {
            Method method = compressorClass.getMethod("create", Map.class);
            ICompressor compressor = (ICompressor)method.invoke(null, compressionOptions);
            // Check for unknown options
            for (String provided : compressionOptions.keySet())
                if (!compressor.supportedOptions().contains(provided))
                    throw new ConfigurationException("Unknown compression options " + provided);
            return compressor;
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
            if (e.getTargetException() instanceof ConfigurationException)
                throw (ConfigurationException) e.getTargetException();

            Throwable cause = e.getCause() == null
                            ? e
                            : e.getCause();

            throw new ConfigurationException(format("%s.create() threw an error: %s %s",
                                                    compressorClass.getSimpleName(),
                                                    cause.getClass().getName(),
                                                    cause.getMessage()),
                                             e);
        }
        catch (ExceptionInInitializerError e)
        {
            throw new ConfigurationException("Cannot initialize class " + compressorClass.getName());
        }
    }

    public static ICompressor createCompressor(ParameterizedClass compression) throws ConfigurationException {
        return createCompressor(parseCompressorClass(compression.class_name), copyOptions(compression.parameters));
    }

    private static Map<String, String> copyOptions(Map<? extends CharSequence, ? extends CharSequence> co)
    {
        if (co == null || co.isEmpty())
            return Collections.emptyMap();

        Map<String, String> compressionOptions = new HashMap<>();
        for (Map.Entry<? extends CharSequence, ? extends CharSequence> entry : co.entrySet())
            compressionOptions.put(entry.getKey().toString(), entry.getValue().toString());
        return compressionOptions;
    }

    /**
     * Parse the chunk length (in KB) and returns it as bytes.
     * 
     * @param chLengthKB the length of the chunk to parse
     * @return the chunk length in bytes
     * @throws ConfigurationException if the chunk size is too large
     */
    private static Integer parseChunkLength(String chLengthKB) throws ConfigurationException
    {
        if (chLengthKB == null)
            return null;

        try
        {
            int parsed = Integer.parseInt(chLengthKB);
            if (parsed > Integer.MAX_VALUE / 1024)
                throw new ConfigurationException(format("Value of %s is too large (%s)", CHUNK_LENGTH_IN_KB,parsed));
            return 1024 * parsed;
        }
        catch (NumberFormatException e)
        {
            throw new ConfigurationException("Invalid value for " + CHUNK_LENGTH_IN_KB, e);
        }
    }

    /**
     * Removes the chunk length option from the specified set of option.
     *
     * @param options the options
     * @return the chunk length value
     */
    private static Integer removeChunkLength(Map<String, String> options)
    {
        if (options.containsKey(CHUNK_LENGTH_IN_KB))
        {
            if (options.containsKey(CHUNK_LENGTH_KB))
            {
                throw new ConfigurationException(format("The '%s' option must not be used if the chunk length is already specified by the '%s' option",
                                                        CHUNK_LENGTH_KB,
                                                        CHUNK_LENGTH_IN_KB));
            }

            return parseChunkLength(options.remove(CHUNK_LENGTH_IN_KB));
        }

        if (options.containsKey(CHUNK_LENGTH_KB))
        {
            if (!hasLoggedChunkLengthWarning)
            {
                hasLoggedChunkLengthWarning = true;
                logger.warn(format("The %s option has been deprecated. You should use %s instead",
                                   CHUNK_LENGTH_KB,
                                   CHUNK_LENGTH_IN_KB));
            }

            return parseChunkLength(options.remove(CHUNK_LENGTH_KB));
        }

        return null;
    }

    /**
     * Returns {@code true} if the specified options contains the name of the compression class to be used,
     * {@code false} otherwise.
     *
     * @param options the options
     * @return {@code true} if the specified options contains the name of the compression class to be used,
     * {@code false} otherwise.
     */
    public static boolean containsSstableCompressionClass(Map<String, String> options)
    {
        return options.containsKey(CLASS) || options.containsKey(SSTABLE_COMPRESSION);
    }

    /**
     * Removes the option specifying the name of the compression class
     *
     * @param options the options
     * @return the name of the compression class
     */
    private static String removeSstableCompressionClass(Map<String, String> options)
    {
        if (options.containsKey(CLASS))
        {
            if (options.containsKey(SSTABLE_COMPRESSION))
                throw new ConfigurationException(format("The '%s' option must not be used if the compression algorithm is already specified by the '%s' option",
                                                        SSTABLE_COMPRESSION,
                                                        CLASS));

            String clazz = options.remove(CLASS);
            if (clazz.isEmpty())
                throw new ConfigurationException(format("The '%s' option must not be empty. To disable compression use 'enabled' : false", CLASS));

            return clazz;
        }

        if (options.containsKey(SSTABLE_COMPRESSION) && !hasLoggedSsTableCompressionWarning)
        {
            hasLoggedSsTableCompressionWarning = true;
            logger.warn(format("The %s option has been deprecated. You should use %s instead",
                               SSTABLE_COMPRESSION,
                               CLASS));
        }

        return options.remove(SSTABLE_COMPRESSION);
    }

    /**
     * Returns {@code true} if the options contains the {@code enabled} option and that its value is
     * {@code true}, otherwise returns {@code false}.
     *
     * @param options the options
     * @return {@code true} if the options contains the {@code enabled} option and that its value is
     * {@code true}, otherwise returns {@code false}.
     */
    public static boolean isEnabled(Map<String, String> options)
    {
        String enabled = options.get(ENABLED);
        return enabled == null || Boolean.parseBoolean(enabled);
    }

    /**
     * Removes the {@code enabled} option from the specified options.
     *
     * @param options the options
     * @return the value of the {@code enabled} option
     */
    private static boolean removeEnabled(Map<String, String> options)
    {
        String enabled = options.remove(ENABLED);
        return enabled == null || Boolean.parseBoolean(enabled);
    }

    // chunkLength must be a power of 2 because we assume so when
    // computing the chunk number from an uncompressed file offset (see
    // CompressedRandomAccessReader.decompresseChunk())
    public void validate() throws ConfigurationException
    {
        // if chunk length was not set (chunkLength == null), this is fine, default will be used
        if (chunkLength != null)
        {
            if (chunkLength <= 0)
                throw new ConfigurationException("Invalid negative or null " + CHUNK_LENGTH_IN_KB);

            int c = chunkLength;
            boolean found = false;
            while (c != 0)
            {
                if ((c & 0x01) != 0)
                {
                    if (found)
                        throw new ConfigurationException(CHUNK_LENGTH_IN_KB + " must be a power of 2");
                    else
                        found = true;
                }
                c >>= 1;
            }
        }
    }

    public Map<String, String> asMap()
    {
        if (!isEnabled())
            return Collections.singletonMap(ENABLED, "false");

        Map<String, String> options = new HashMap<>(otherOptions);
        options.put(CLASS, sstableCompressor.getClass().getName());
        options.put(CHUNK_LENGTH_IN_KB, chunkLengthInKB());

        return options;
    }

    public String chunkLengthInKB()
    {
        return String.valueOf(chunkLength() / 1024);
    }

    public void setCrcCheckChance(double crcCheckChance)
    {
        this.crcCheckChance = crcCheckChance;
    }

    public double getCrcCheckChance()
    {
        return crcCheckChance;
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

        CompressionParams cp = (CompressionParams) obj;
        return new EqualsBuilder()
            .append(sstableCompressor, cp.sstableCompressor)
            .append(chunkLength(), cp.chunkLength())
            .append(otherOptions, cp.otherOptions)
            .isEquals();
    }

    @Override
    public int hashCode()
    {
        return new HashCodeBuilder(29, 1597)
            .append(sstableCompressor)
            .append(chunkLength())
            .append(otherOptions)
            .toHashCode();
    }

    static class Serializer implements IVersionedSerializer<CompressionParams>
    {
        public void serialize(CompressionParams parameters, DataOutputPlus out, int version) throws IOException
        {
            out.writeUTF(parameters.sstableCompressor.getClass().getSimpleName());
            out.writeInt(parameters.otherOptions.size());
            for (Map.Entry<String, String> entry : parameters.otherOptions.entrySet())
            {
                out.writeUTF(entry.getKey());
                out.writeUTF(entry.getValue());
            }
            out.writeInt(parameters.chunkLength());
        }

        public CompressionParams deserialize(DataInputPlus in, int version) throws IOException
        {
            String compressorName = in.readUTF();
            int optionCount = in.readInt();
            Map<String, String> options = new HashMap<>();
            for (int i = 0; i < optionCount; ++i)
            {
                String key = in.readUTF();
                String value = in.readUTF();
                options.put(key, value);
            }
            int chunkLength = in.readInt();
            CompressionParams parameters;
            try
            {
                parameters = new CompressionParams(compressorName, chunkLength, options);
            }
            catch (ConfigurationException e)
            {
                throw new RuntimeException("Cannot create CompressionParams for parameters", e);
            }
            return parameters;
        }

        public long serializedSize(CompressionParams parameters, int version)
        {
            long size = TypeSizes.sizeof(parameters.sstableCompressor.getClass().getSimpleName());
            size += TypeSizes.sizeof(parameters.otherOptions.size());
            for (Map.Entry<String, String> entry : parameters.otherOptions.entrySet())
            {
                size += TypeSizes.sizeof(entry.getKey());
                size += TypeSizes.sizeof(entry.getValue());
            }
            size += TypeSizes.sizeof(parameters.chunkLength());
            return size;
        }
    }
}
