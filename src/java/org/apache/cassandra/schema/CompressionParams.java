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
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableMap;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DataStorageSpec;
import org.apache.cassandra.config.DatabaseDescriptor;
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
    private static final Logger logger = LoggerFactory.getLogger(CompressionParams.class);

    public static final int DEFAULT_CHUNK_LENGTH = 1024 * 16; // in KB
    public static final double DEFAULT_MIN_COMPRESS_RATIO = 0.0;        // Since pre-4.0 versions do not understand the
                                                                        // new compression parameter we can't use a
                                                                        // different default value.
    public static final IVersionedSerializer<CompressionParams> serializer = new Serializer();

    public static final String CLASS = "class";
    public static final String CHUNK_LENGTH_IN_KB = "chunk_length_in_kb";
    /**
     * Requires a DataStorageSpec suffix
     */
    public static final String CHUNK_LENGTH = "chunk_length";
    /**
     * Requires a DataStorageSpec suffix
     */
    public static final String MAX_COMPRESSED_LENGTH = "max_compressed_length";
    public static final String ENABLED = "enabled";
    public static final String MIN_COMPRESS_RATIO = "min_compress_ratio";

    public static final CompressionParams NOOP = new CompressionParams(NoopCompressor.create(Collections.emptyMap()),
                                                                       // 4 KiB is often the underlying disk block size
                                                                       1024 * 4,
                                                                       Integer.MAX_VALUE,
                                                                       DEFAULT_MIN_COMPRESS_RATIO,
                                                                       Collections.emptyMap());

    private static final CompressionParams DEFAULT = new CompressionParams(LZ4Compressor.create(Collections.<String, String>emptyMap()),
                                                                       DEFAULT_CHUNK_LENGTH,
                                                                       calcMaxCompressedLength(DEFAULT_CHUNK_LENGTH, DEFAULT_MIN_COMPRESS_RATIO),
                                                                       DEFAULT_MIN_COMPRESS_RATIO,
                                                                       Collections.emptyMap());
    @VisibleForTesting
    static final String TOO_MANY_CHUNK_LENGTH = "Only one of 'chunk_length', or 'chunk_length_in_kb' may be specified";

    private final ICompressor sstableCompressor;
    /**
     * THe chunk length in KB
     */
    private final int chunkLength;
    /**
     * The compressed length in KB.
     * In content we store max length to avoid rounding errors causing compress/decompress mismatch.
     */
    private final int maxCompressedLength;
    /**
     * The minimum compression ratio.
     * In configuration we store min ratio, the input parameter.
     * Ths is mathematically related to chunkLength and maxCompressedLength in that
     * # chunk_length / max_compressed_length = min_compress_ratio
     */
    private final double minCompressRatio;
    private final ImmutableMap<String, String> otherOptions; // Unrecognized options, can be used by the compressor

    // TODO: deprecated, should now be carefully removed. Doesn't affect schema code as it isn't included in equals() and hashCode()
    private volatile double crcCheckChance = 1.0;


    public enum CompressorType
    {
        lz4(LZ4Compressor.class.getName(), LZ4Compressor::create),
        noop(NoopCompressor.class.getName(), NoopCompressor::create),
        snappy(SnappyCompressor.class.getName(), SnappyCompressor::create),
        deflate(DeflateCompressor.class.getName(), DeflateCompressor::create),
        zstd(ZstdCompressor.class.getName(), ZstdCompressor::create),
        none(null, (opt) -> null);

        final String className;
        final Function<Map<String,String>,ICompressor> creator;

        CompressorType(String className, Function<Map<String,String>,ICompressor> creator) {
            this.className = className;
            this.creator = creator;
        }

        static CompressorType forClass(String name) {
            if (name == null)
                return none;

            for (CompressorType type : CompressorType.values()) {
                if (Objects.equal(type.className, name))
                    return type;
            }
            return null;
        }

        public ICompressor create(Map<String,String> options) {
            return creator.apply(options);
        }
    }

    public static CompressionParams defaultParams()
    {
        return fromParameterizedClass(DatabaseDescriptor.getSSTableCompressionOptions());
    }

    public static CompressionParams fromParameterizedClass(ParameterizedClass options)
    {
        if (options == null)
            return CassandraRelevantProperties.DETERMINISM_SSTABLE_COMPRESSION_DEFAULT.getBoolean()
                   ? DEFAULT
                   : noCompression();

        return fromClassAndOptions(options.class_name, options.parameters == null ? Collections.emptyMap() : copyOptions(options.parameters));
    }

    private static String invalidValue(String param, Object value)
    {
        return format("Invalid '%s' value for the 'compression' option: %s", param, value);
    }

    private static String invalidValue(String param, String extraText, Object value) {
        return format("Invalid '%s' value for the 'compression' option.  %s: %s", param, extraText, value );
    }

    public static CompressionParams fromMap(Map<String, String> opts)
    {
        Map<String, String> options = copyOptions(opts);

        String sstableCompressionClass = removeSstableCompressionClass(options);

        return fromClassAndOptions(sstableCompressionClass, options);
    }

    private static CompressionParams fromClassAndOptions(String sstableCompressionClass, Map<String,String> options)
    {
        final boolean enabled = removeEnabled(options);

        final int chunk_length_in_kb = removeChunkLength(options);

        // figure out how we calculate the max_compressed_length and min_compress_ratio
        if (options.containsKey(MIN_COMPRESS_RATIO)  && options.containsKey(MAX_COMPRESSED_LENGTH))
            throw new ConfigurationException("Can not specify both 'min_compress_ratio' and 'max_compressed_length' for the compressor parameters.");

        // calculate the max_compressed_length and min_compress_ratio
        int max_compressed_length;
        double min_compress_ratio;
        String max_compressed_length_str = options.remove(MAX_COMPRESSED_LENGTH);
        if (!StringUtils.isBlank(max_compressed_length_str))
        {
            try
            {
                max_compressed_length = new DataStorageSpec.IntKibibytesBound(max_compressed_length_str).toKibibytes();
                validateMaxCompressedLength(max_compressed_length, chunk_length_in_kb);
                min_compress_ratio = CompressionParams.calcMinCompressRatio(chunk_length_in_kb, max_compressed_length);
            } catch (IllegalArgumentException e) {
                throw new ConfigurationException(invalidValue(MAX_COMPRESSED_LENGTH, e.getMessage(), max_compressed_length_str));
            }
        }
        else
        {
            min_compress_ratio = removeMinCompressRatio(options);
            validateMinCompressRatio(min_compress_ratio);
            max_compressed_length =  CompressionParams.calcMaxCompressedLength(chunk_length_in_kb,min_compress_ratio);
        }

        // try to set compressor type
        CompressorType compressorType = StringUtils.isEmpty(sstableCompressionClass) ? CompressorType.lz4 : null;
        if (compressorType == null)
        {
            try
            {
                compressorType = CompressorType.valueOf(sstableCompressionClass);
            }
            catch (IllegalArgumentException expected)
            {
                compressorType = CompressorType.forClass(sstableCompressionClass);
            }
        }

        Function<Map<String,String>,ICompressor> creator = compressorType  != null ? compressorType.creator : (opt) -> createCompressor(parseCompressorClass(sstableCompressionClass), opt);
        CompressionParams cp = new CompressionParams(enabled ? creator.apply(options) : null, chunk_length_in_kb, max_compressed_length, min_compress_ratio, options);
        if (enabled && compressorType != CompressorType.none && cp.getSstableCompressor() == null)
            throw new ConfigurationException(format("'%s' is not a valid compressor class name for the 'sstable_compressor' option.", sstableCompressionClass));

        return cp;
    }

    public static CompressionParams noCompression()
    {
        return new CompressionParams((ICompressor) null, DEFAULT_CHUNK_LENGTH, Integer.MAX_VALUE, DEFAULT_MIN_COMPRESS_RATIO, Collections.emptyMap());
    }

    public Class<? extends ICompressor> klass()
    {
        return sstableCompressor.getClass();
    }

    public CompressionParams(String sstableCompressorClass, Map<String, String> otherOptions, int chunkLength, double minCompressRatio) throws ConfigurationException
    {
        this(createCompressor(parseCompressorClass(sstableCompressorClass), otherOptions), chunkLength, calcMaxCompressedLength(chunkLength, minCompressRatio), minCompressRatio, otherOptions);
        validateChunkLength(CHUNK_LENGTH, chunkLength);
        validateMinCompressRatio(minCompressRatio);
    }

    public static int calcMaxCompressedLength(int chunkLength, double minCompressRatio)
    {
        return (int) Math.ceil(Math.min(chunkLength / minCompressRatio, Integer.MAX_VALUE));
    }

    public CompressionParams(String sstableCompressorClass, int chunkLength, int maxCompressedLength, Map<String, String> otherOptions) throws ConfigurationException
    {
        this(createCompressor(parseCompressorClass(sstableCompressorClass), otherOptions), chunkLength, maxCompressedLength, calcMinCompressRatio(chunkLength, maxCompressedLength), otherOptions);
        validateChunkLength(CHUNK_LENGTH, chunkLength);
        validateMaxCompressedLength(maxCompressedLength, chunkLength);
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

    double minCompressRatio()
    {
        return minCompressRatio;
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
     * Removes the chunk length option from the specified set of option.
     *
     * @param options the options
     * @return the chunk length value
     */
    private static int removeChunkLength(Map<String, String> options)
    {
        Integer chunk_length_in_kb = null;
        String key = null;
        if (options.containsKey(CHUNK_LENGTH)) {
            key = CHUNK_LENGTH;
            String value = options.remove(CHUNK_LENGTH);
            try
            {
                chunk_length_in_kb =  new DataStorageSpec.IntKibibytesBound(value).toKibibytes();
            } catch (IllegalArgumentException e) {
                throw new ConfigurationException(invalidValue(CHUNK_LENGTH, e.getMessage(), value));
            }
        }

        if (options.containsKey(CHUNK_LENGTH_IN_KB))
        {
            key = CHUNK_LENGTH_IN_KB;
            if (chunk_length_in_kb != null)
                throw new ConfigurationException(TOO_MANY_CHUNK_LENGTH);
            else
            {
                String chLengthKB = options.remove(CHUNK_LENGTH_IN_KB);
                try
                {
                    int parsed = Integer.parseInt(chLengthKB);
                    if (parsed > Integer.MAX_VALUE / 1024)
                        throw new ConfigurationException(invalidValue(CHUNK_LENGTH_IN_KB, "Value is too large", parsed));
                    if (parsed <= 0)
                        throw new ConfigurationException(invalidValue(CHUNK_LENGTH_IN_KB, "May not be <= 0", parsed));
                    chunk_length_in_kb = 1024 * parsed;
                }
                catch (NumberFormatException e)
                {
                    throw new ConfigurationException(invalidValue(CHUNK_LENGTH_IN_KB, e.getMessage(), chLengthKB));
                }
            }
        }

        if (chunk_length_in_kb != null) {
            int chunkLength = chunk_length_in_kb.intValue() ;
            validateChunkLength(key, chunkLength);
            return chunkLength;
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
        return  ratio != null ? Double.parseDouble(ratio) : DEFAULT_MIN_COMPRESS_RATIO;
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
        return options.containsKey(CLASS);
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
            String clazz = options.remove(CLASS);
            if (clazz.isEmpty())
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

    private static void validateChunkLength(String key, int chunkLength) throws ConfigurationException{
        if (chunkLength <= 0)
            throw new ConfigurationException(invalidValue(key, "May not be <= 0", chunkLength));

        // chunkLength must be a power of 2 because we assume so when
        // computing the chunk number from an uncompressed file offset (see
        // CompressedRandomAccessReader.decompresseChunk())
        if ((chunkLength & (chunkLength - 1)) != 0)
            throw new ConfigurationException(invalidValue(key,  "Must be a power of 2", chunkLength));
    }

    private static void validateMinCompressRatio(double ratio) throws ConfigurationException {
        if (ratio != DEFAULT_MIN_COMPRESS_RATIO && ratio < 1.0)
            throw new ConfigurationException(invalidValue(MIN_COMPRESS_RATIO , "Can either be 0 or greater than or equal to 1", ratio));

    }

    private static void validateMaxCompressedLength(int maxCompressedLength, int chunkLength) throws ConfigurationException {
        if (maxCompressedLength < 0)
            throw new ConfigurationException(invalidValue(MAX_COMPRESSED_LENGTH, "May not be less than zero", maxCompressedLength));

        if (maxCompressedLength > chunkLength && maxCompressedLength < Integer.MAX_VALUE)
            throw new ConfigurationException(invalidValue(MAX_COMPRESSED_LENGTH, "Must be less than or equal to chunk length"));
    }

    public void validate() throws ConfigurationException
    {
        validateChunkLength(CHUNK_LENGTH, chunkLength);
        validateMinCompressRatio(minCompressRatio);
        // do not need to validate max_compressed_length as the above 2 checks limit all possibilities of max_compressed_length
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

    public void setCrcCheckChance(double crcCheckChance)
    {
        this.crcCheckChance = crcCheckChance;
    }

    public double getCrcCheckChance()
    {
        return crcCheckChance;
    }

    public boolean shouldCheckCrc()
    {
        double checkChance = getCrcCheckChance();
        return checkChance >= 1d ||
               (checkChance > 0d && checkChance > ThreadLocalRandom.current().nextDouble());
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
            out.writeUTF(parameters.sstableCompressor.getClass().getSimpleName());
            out.writeInt(parameters.otherOptions.size());
            for (Map.Entry<String, String> entry : parameters.otherOptions.entrySet())
            {
                out.writeUTF(entry.getKey());
                out.writeUTF(entry.getValue());
            }
            out.writeInt(parameters.chunkLength());
            if (version >= MessagingService.VERSION_40)
                out.writeInt(parameters.maxCompressedLength);
            else
                if (parameters.maxCompressedLength != Integer.MAX_VALUE)
                    throw new UnsupportedOperationException("Cannot stream SSTables with uncompressed chunks to pre-4.0 nodes.");
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
            int minCompressRatio = Integer.MAX_VALUE;   // Earlier Cassandra cannot use uncompressed chunks.
            if (version >= MessagingService.VERSION_40)
                minCompressRatio = in.readInt();

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
            long size = TypeSizes.sizeof(parameters.sstableCompressor.getClass().getSimpleName());
            size += TypeSizes.sizeof(parameters.otherOptions.size());
            for (Map.Entry<String, String> entry : parameters.otherOptions.entrySet())
            {
                size += TypeSizes.sizeof(entry.getKey());
                size += TypeSizes.sizeof(entry.getValue());
            }
            size += TypeSizes.sizeof(parameters.chunkLength());
            if (version >= MessagingService.VERSION_40)
                size += TypeSizes.sizeof(parameters.maxCompressedLength());
            return size;
        }
    }
}
