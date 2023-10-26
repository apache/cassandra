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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableMap;

import org.apache.commons.lang3.builder.HashCodeBuilder;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.ParameterizedClass;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.compress.*;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.MessagingService;

import static java.lang.String.format;

public final class CompressionParams
{
    public static final int DEFAULT_CHUNK_LENGTH = 1024 * 16;
    public static final double DEFAULT_MIN_COMPRESS_RATIO = 0.0;        // Since pre-4.0 versions do not understand the
                                                                        // new compression parameter we can't use a
                                                                        // different default value.
    public static final IVersionedSerializer<CompressionParams> serializer = new Serializer();

    public static final String CLASS = "class";
    public static final String CHUNK_LENGTH_IN_KB = "chunk_length_in_kb";
    public static final String ENABLED = "enabled";
    public static final String MIN_COMPRESS_RATIO = "min_compress_ratio";

    public static final CompressionParams DEFAULT = !CassandraRelevantProperties.DETERMINISM_SSTABLE_COMPRESSION_DEFAULT.getBoolean()
                                                    ? noCompression()
                                                    : new CompressionParams(LZ4Compressor.create(Collections.emptyMap()),
                                                                            DEFAULT_CHUNK_LENGTH,
                                                                            calcMaxCompressedLength(DEFAULT_CHUNK_LENGTH, DEFAULT_MIN_COMPRESS_RATIO),
                                                                            DEFAULT_MIN_COMPRESS_RATIO,
                                                                            Collections.emptyMap());

    public static final CompressionParams NOOP = new CompressionParams(NoopCompressor.create(Collections.emptyMap()),
                                                                       // 4 KiB is often the underlying disk block size
                                                                       1024 * 4,
                                                                       Integer.MAX_VALUE,
                                                                       DEFAULT_MIN_COMPRESS_RATIO,
                                                                       Collections.emptyMap());

    private final ICompressor sstableCompressor;
    private final int chunkLength;
    private final int maxCompressedLength;  // In content we store max length to avoid rounding errors causing compress/decompress mismatch.
    private final double minCompressRatio;  // In configuration we store min ratio, the input parameter.
    private final ImmutableMap<String, String> otherOptions; // Unrecognized options, can be used by the compressor

    public static CompressionParams fromMap(Map<String, String> opts)
    {
        Map<String, String> options = copyOptions(opts);

        String sstableCompressionClass;

        if (!opts.isEmpty() && isEnabled(opts) && !options.containsKey(CLASS))
            throw new ConfigurationException(format("Missing sub-option '%s' for the 'compression' option.", CLASS));

        if (!removeEnabled(options) && !options.isEmpty())
            throw new ConfigurationException(format("If the '%s' option is set to false no other options must be specified", ENABLED));
        else
            sstableCompressionClass = removeSSTableCompressionClass(options);

        int chunkLength = removeChunkLength(options);
        double minCompressRatio = removeMinCompressRatio(options);

        CompressionParams cp = new CompressionParams(sstableCompressionClass, options, chunkLength, minCompressRatio);
        cp.validate();

        return cp;
    }

    public Class<? extends ICompressor> klass()
    {
        return sstableCompressor.getClass();
    }

    public static CompressionParams noCompression()
    {
        return new CompressionParams(null, DEFAULT_CHUNK_LENGTH, Integer.MAX_VALUE, 0.0, Collections.emptyMap());
    }

    // The shorthand methods below are only used for tests. They are a little inconsistent in their choice of
    // parameters -- this is done on purpose to test out various compression parameter combinations.

    @VisibleForTesting
    public static CompressionParams snappy()
    {
        return snappy(DEFAULT_CHUNK_LENGTH);
    }

    @VisibleForTesting
    public static CompressionParams snappy(int chunkLength)
    {
        return snappy(chunkLength, 1.1);
    }

    @VisibleForTesting
    public static CompressionParams snappy(int chunkLength, double minCompressRatio)
    {
        return new CompressionParams(SnappyCompressor.instance, chunkLength, calcMaxCompressedLength(chunkLength, minCompressRatio), minCompressRatio, Collections.emptyMap());
    }

    @VisibleForTesting
    public static CompressionParams deflate()
    {
        return deflate(DEFAULT_CHUNK_LENGTH);
    }

    @VisibleForTesting
    public static CompressionParams deflate(int chunkLength)
    {
        return new CompressionParams(DeflateCompressor.instance, chunkLength, Integer.MAX_VALUE, 0.0, Collections.emptyMap());
    }

    @VisibleForTesting
    public static CompressionParams lz4()
    {
        return lz4(DEFAULT_CHUNK_LENGTH);
    }

    @VisibleForTesting
    public static CompressionParams lz4(int chunkLength)
    {
        return lz4(chunkLength, chunkLength);
    }

    @VisibleForTesting
    public static CompressionParams lz4(int chunkLength, int maxCompressedLength)
    {
        return new CompressionParams(LZ4Compressor.create(Collections.emptyMap()), chunkLength, maxCompressedLength, calcMinCompressRatio(chunkLength, maxCompressedLength), Collections.emptyMap());
    }

    public static CompressionParams zstd()
    {
        return zstd(DEFAULT_CHUNK_LENGTH);
    }

    public static CompressionParams zstd(Integer chunkLength)
    {
        ZstdCompressor compressor = ZstdCompressor.create(Collections.emptyMap());
        return new CompressionParams(compressor, chunkLength, Integer.MAX_VALUE, DEFAULT_MIN_COMPRESS_RATIO, Collections.emptyMap());
    }

    @VisibleForTesting
    public static CompressionParams noop()
    {
        NoopCompressor compressor = NoopCompressor.create(Collections.emptyMap());
        return new CompressionParams(compressor, DEFAULT_CHUNK_LENGTH, Integer.MAX_VALUE, DEFAULT_MIN_COMPRESS_RATIO, Collections.emptyMap());
    }

    public CompressionParams(String sstableCompressorClass, Map<String, String> otherOptions, int chunkLength, double minCompressRatio) throws ConfigurationException
    {
        this(createCompressor(parseCompressorClass(sstableCompressorClass), otherOptions), chunkLength, calcMaxCompressedLength(chunkLength, minCompressRatio), minCompressRatio, otherOptions);
    }

    static int calcMaxCompressedLength(int chunkLength, double minCompressRatio)
    {
        return (int) Math.ceil(Math.min(chunkLength / minCompressRatio, Integer.MAX_VALUE));
    }

    public CompressionParams(String sstableCompressorClass, int chunkLength, int maxCompressedLength, Map<String, String> otherOptions) throws ConfigurationException
    {
        this(createCompressor(parseCompressorClass(sstableCompressorClass), otherOptions), chunkLength, maxCompressedLength, calcMinCompressRatio(chunkLength, maxCompressedLength), otherOptions);
    }

    static double calcMinCompressRatio(int chunkLength, int maxCompressedLength)
    {
        if (maxCompressedLength == Integer.MAX_VALUE)
            return 0;
        return chunkLength * 1.0 / maxCompressedLength;
    }

    private CompressionParams(ICompressor sstableCompressor, int chunkLength, int maxCompressedLength, double minCompressRatio, Map<String, String> otherOptions) throws ConfigurationException
    {
        this.sstableCompressor = sstableCompressor;
        this.chunkLength = chunkLength;
        this.otherOptions = ImmutableMap.copyOf(otherOptions);
        this.minCompressRatio = minCompressRatio;
        this.maxCompressedLength = maxCompressedLength;
    }

    public CompressionParams copy()
    {
        return new CompressionParams(sstableCompressor, chunkLength, maxCompressedLength, minCompressRatio, otherOptions);
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
        return chunkLength;
    }

    public int maxCompressedLength()
    {
        return maxCompressedLength;
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

    public static ICompressor createCompressor(ParameterizedClass compression) throws ConfigurationException
    {
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
     * Parse the chunk length (in KiB) and returns it as bytes.
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
    private static int removeChunkLength(Map<String, String> options)
    {
        if (options.containsKey(CHUNK_LENGTH_IN_KB))
        {
            return parseChunkLength(options.remove(CHUNK_LENGTH_IN_KB));
        }

        return DEFAULT_CHUNK_LENGTH;
    }

    /**
     * Removes the min compress ratio option from the specified set of option.
     *
     * @param options the options
     * @return the min compress ratio, used to calculate max chunk size to write compressed
     */
    private static double removeMinCompressRatio(Map<String, String> options)
    {
        String ratio = options.remove(MIN_COMPRESS_RATIO);
        if (ratio != null)
        {
            return Double.parseDouble(ratio);
        }
        return DEFAULT_MIN_COMPRESS_RATIO;
    }

    /**
     * Removes the option specifying the name of the compression class
     *
     * @param options the options
     * @return the name of the compression class
     */
    private static String removeSSTableCompressionClass(Map<String, String> options)
    {
        if (options.containsKey(CLASS))
        {
            String clazz = options.remove(CLASS);

            if (clazz == null || clazz.isEmpty())
                throw new ConfigurationException(format("The '%s' option must not be empty. To disable compression use 'enabled' : false", CLASS));

            return clazz;
        }

        return null;
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
        if (chunkLength <= 0)
            throw new ConfigurationException("Invalid negative or null " + CHUNK_LENGTH_IN_KB);

        if ((chunkLength & (chunkLength - 1)) != 0)
            throw new ConfigurationException(CHUNK_LENGTH_IN_KB + " must be a power of 2");

        if (maxCompressedLength < 0)
            throw new ConfigurationException("Invalid negative " + MIN_COMPRESS_RATIO);

        if (maxCompressedLength > chunkLength && maxCompressedLength < Integer.MAX_VALUE)
            throw new ConfigurationException(MIN_COMPRESS_RATIO + " can either be 0 or greater than or equal to 1");
    }

    public Map<String, String> asMap()
    {
        if (!isEnabled())
            return Collections.singletonMap(ENABLED, "false");

        Map<String, String> options = new HashMap<>(otherOptions);
        options.put(CLASS, sstableCompressor.getClass().getName());
        options.put(CHUNK_LENGTH_IN_KB, chunkLengthInKB());
        if (minCompressRatio != DEFAULT_MIN_COMPRESS_RATIO)
            options.put(MIN_COMPRESS_RATIO, String.valueOf(minCompressRatio));

        return options;
    }

    public String chunkLengthInKB()
    {
        return String.valueOf(chunkLength() / 1024);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj == this)
            return true;

        if (!(obj instanceof CompressionParams))
            return false;

        CompressionParams cp = (CompressionParams) obj;

        return Objects.equal(sstableCompressor, cp.sstableCompressor)
            && chunkLength == cp.chunkLength
            && otherOptions.equals(cp.otherOptions)
            && minCompressRatio == cp.minCompressRatio;
    }

    @Override
    public int hashCode()
    {
        return new HashCodeBuilder(29, 1597)
            .append(sstableCompressor)
            .append(chunkLength)
            .append(otherOptions)
            .append(minCompressRatio)
            .toHashCode();
    }

    static class Serializer implements IVersionedSerializer<CompressionParams>
    {
        public void serialize(CompressionParams parameters, DataOutputPlus out, int version) throws IOException
        {
            assert version >= MessagingService.VERSION_40;
            out.writeUTF(parameters.sstableCompressor.getClass().getSimpleName());
            out.writeInt(parameters.otherOptions.size());
            for (Map.Entry<String, String> entry : parameters.otherOptions.entrySet())
            {
                out.writeUTF(entry.getKey());
                out.writeUTF(entry.getValue());
            }
            out.writeInt(parameters.chunkLength());
            out.writeInt(parameters.maxCompressedLength);
        }

        public CompressionParams deserialize(DataInputPlus in, int version) throws IOException
        {
            assert version >= MessagingService.VERSION_40;
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
            int minCompressRatio = in.readInt();

            CompressionParams parameters;
            try
            {
                parameters = new CompressionParams(compressorName, chunkLength, minCompressRatio, options);
            }
            catch (ConfigurationException e)
            {
                throw new RuntimeException("Cannot create CompressionParams for parameters", e);
            }
            return parameters;
        }

        public long serializedSize(CompressionParams parameters, int version)
        {
            assert version >= MessagingService.VERSION_40;
            long size = TypeSizes.sizeof(parameters.sstableCompressor.getClass().getSimpleName());
            size += TypeSizes.sizeof(parameters.otherOptions.size());
            for (Map.Entry<String, String> entry : parameters.otherOptions.entrySet())
            {
                size += TypeSizes.sizeof(entry.getKey());
                size += TypeSizes.sizeof(entry.getValue());
            }
            size += TypeSizes.sizeof(parameters.chunkLength());
            size += TypeSizes.sizeof(parameters.maxCompressedLength());
            return size;
        }
    }
}
