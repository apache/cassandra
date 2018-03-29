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
package org.apache.cassandra.hints;

import java.io.DataInput;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.regex.Pattern;
import java.util.zip.CRC32;
import javax.crypto.Cipher;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.ParameterizedClass;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.io.compress.ICompressor;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.CompressionParams;
import org.apache.cassandra.security.EncryptionContext;
import org.apache.cassandra.utils.Hex;
import org.json.simple.JSONValue;

import static org.apache.cassandra.utils.FBUtilities.updateChecksumInt;

/**
 * Describes the host id, the version, the timestamp of creation, and an arbitrary map of JSON-encoded parameters of a
 * hints file.
 *
 * Written in the beginning of each hints file.
 */
final class HintsDescriptor
{
    private static final Logger logger = LoggerFactory.getLogger(HintsDescriptor.class);

    static final int VERSION_30 = 1;
    static final int CURRENT_VERSION = VERSION_30;

    static final String COMPRESSION = "compression";
    static final String ENCRYPTION = "encryption";

    static final Pattern pattern =
        Pattern.compile("^[a-fA-F0-9]{8}\\-[a-fA-F0-9]{4}\\-[a-fA-F0-9]{4}\\-[a-fA-F0-9]{4}\\-[a-fA-F0-9]{12}\\-(\\d+)\\-(\\d+)\\.hints$");

    final UUID hostId;
    final int version;
    final long timestamp;

    final ImmutableMap<String, Object> parameters;
    final ParameterizedClass compressionConfig;

    private final Cipher cipher;
    private final ICompressor compressor;

    HintsDescriptor(UUID hostId, int version, long timestamp, ImmutableMap<String, Object> parameters)
    {
        this.hostId = hostId;
        this.version = version;
        this.timestamp = timestamp;
        compressionConfig = createCompressionConfig(parameters);

        EncryptionData encryption = createEncryption(parameters);
        if (encryption == null)
        {
            cipher = null;
            compressor = null;
        }
        else
        {
            if (compressionConfig != null)
                throw new IllegalStateException("a hints file cannot be configured for both compression and encryption");
            cipher = encryption.cipher;
            compressor = encryption.compressor;
            parameters = encryption.params;
        }

        this.parameters = parameters;
    }

    HintsDescriptor(UUID hostId, long timestamp, ImmutableMap<String, Object> parameters)
    {
        this(hostId, CURRENT_VERSION, timestamp, parameters);
    }

    HintsDescriptor(UUID hostId, long timestamp)
    {
        this(hostId, CURRENT_VERSION, timestamp, ImmutableMap.<String, Object>of());
    }

    @SuppressWarnings("unchecked")
    static ParameterizedClass createCompressionConfig(Map<String, Object> params)
    {
        if (params.containsKey(COMPRESSION))
        {
            Map<String, Object> compressorConfig = (Map<String, Object>) params.get(COMPRESSION);
            return new ParameterizedClass((String) compressorConfig.get(ParameterizedClass.CLASS_NAME),
                                          (Map<String, String>) compressorConfig.get(ParameterizedClass.PARAMETERS));
        }
        else
        {
            return null;
        }
    }

    /**
     * Create, if necessary, the required encryption components (for either decrpyt or encrypt operations).
     * Note that in the case of encyption (this is, when writing out a new hints file), we need to write
     * the cipher's IV out to the header so it can be used when decrypting. Thus, we need to add an additional
     * entry to the {@code params} map.
     *
     * @param params the base parameters into the descriptor.
     * @return null if not using encryption; else, the initialized {@link Cipher} and a possibly updated version
     * of the {@code params} map.
     */
    @SuppressWarnings("unchecked")
    static EncryptionData createEncryption(ImmutableMap<String, Object> params)
    {
        if (params.containsKey(ENCRYPTION))
        {
            Map<?, ?> encryptionConfig = (Map<?, ?>) params.get(ENCRYPTION);
            EncryptionContext encryptionContext = EncryptionContext.createFromMap(encryptionConfig, DatabaseDescriptor.getEncryptionContext());

            try
            {
                Cipher cipher;
                if (encryptionConfig.containsKey(EncryptionContext.ENCRYPTION_IV))
                {
                    cipher = encryptionContext.getDecryptor();
                }
                else
                {
                    cipher = encryptionContext.getEncryptor();
                    ImmutableMap<String, Object> encParams = ImmutableMap.<String, Object>builder()
                                                                 .putAll(encryptionContext.toHeaderParameters())
                                                                 .put(EncryptionContext.ENCRYPTION_IV, Hex.bytesToHex(cipher.getIV()))
                                                                 .build();

                    Map<String, Object> map = new HashMap<>(params);
                    map.put(ENCRYPTION, encParams);
                    params = ImmutableMap.<String, Object>builder().putAll(map).build();
                }
                return new EncryptionData(cipher, encryptionContext.getCompressor(), params);
            }
            catch (IOException ioe)
            {
                logger.warn("failed to create encyption context for hints file. ignoring encryption for hints.", ioe);
                return null;
            }
        }
        else
        {
            return null;
        }
    }

    private static final class EncryptionData
    {
        final Cipher cipher;
        final ICompressor compressor;
        final ImmutableMap<String, Object> params;

        private EncryptionData(Cipher cipher, ICompressor compressor, ImmutableMap<String, Object> params)
        {
            this.cipher = cipher;
            this.compressor = compressor;
            this.params = params;
        }
    }

    String fileName()
    {
        return String.format("%s-%s-%s.hints", hostId, timestamp, version);
    }

    String checksumFileName()
    {
        return String.format("%s-%s-%s.crc32", hostId, timestamp, version);
    }

    int messagingVersion()
    {
        return messagingVersion(version);
    }

    static int messagingVersion(int hintsVersion)
    {
        switch (hintsVersion)
        {
            case VERSION_30:
                return MessagingService.FORCE_3_0_PROTOCOL_VERSION ? MessagingService.VERSION_30 : MessagingService.VERSION_3014;
            default:
                throw new AssertionError();
        }
    }

    static boolean isHintFileName(Path path)
    {
        return pattern.matcher(path.getFileName().toString()).matches();
    }

    static Optional<HintsDescriptor> readFromFileQuietly(Path path)
    {
        try (RandomAccessFile raf = new RandomAccessFile(path.toFile(), "r"))
        {
            return Optional.of(deserialize(raf));
        }
        catch (ChecksumMismatchException e)
        {
            throw new FSReadError(e, path.toFile());
        }
        catch (IOException e)
        {
            logger.error("Failed to deserialize hints descriptor {}", path.toString(), e);
            return Optional.empty();
        }
    }

    static HintsDescriptor readFromFile(Path path)
    {
        try (RandomAccessFile raf = new RandomAccessFile(path.toFile(), "r"))
        {
            return deserialize(raf);
        }
        catch (IOException e)
        {
            throw new FSReadError(e, path.toFile());
        }
    }

    public boolean isCompressed()
    {
        return compressionConfig != null;
    }

    public boolean isEncrypted()
    {
        return cipher != null;
    }

    public ICompressor createCompressor()
    {
        if (isCompressed())
            return CompressionParams.createCompressor(compressionConfig);
        if (isEncrypted())
            return compressor;
        return null;
    }

    public Cipher getCipher()
    {
        return isEncrypted() ? cipher : null;
    }

    @Override
    public String toString()
    {
        return MoreObjects.toStringHelper(this)
                          .add("hostId", hostId)
                          .add("version", version)
                          .add("timestamp", timestamp)
                          .add("parameters", parameters)
                          .toString();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;

        if (!(o instanceof HintsDescriptor))
            return false;

        HintsDescriptor hd = (HintsDescriptor) o;

        return Objects.equal(hostId, hd.hostId)
            && Objects.equal(version, hd.version)
            && Objects.equal(timestamp, hd.timestamp)
            && Objects.equal(parameters, hd.parameters);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(hostId, version, timestamp, parameters);
    }

    void serialize(DataOutputPlus out) throws IOException
    {
        CRC32 crc = new CRC32();

        out.writeInt(version);
        updateChecksumInt(crc, version);

        out.writeLong(timestamp);
        updateChecksumLong(crc, timestamp);

        out.writeLong(hostId.getMostSignificantBits());
        updateChecksumLong(crc, hostId.getMostSignificantBits());
        out.writeLong(hostId.getLeastSignificantBits());
        updateChecksumLong(crc, hostId.getLeastSignificantBits());

        byte[] paramsBytes = JSONValue.toJSONString(parameters).getBytes(StandardCharsets.UTF_8);
        out.writeInt(paramsBytes.length);
        updateChecksumInt(crc, paramsBytes.length);
        out.writeInt((int) crc.getValue());

        out.write(paramsBytes);
        crc.update(paramsBytes, 0, paramsBytes.length);

        out.writeInt((int) crc.getValue());
    }

    int serializedSize()
    {
        int size = TypeSizes.sizeof(version);
        size += TypeSizes.sizeof(timestamp);

        size += TypeSizes.sizeof(hostId.getMostSignificantBits());
        size += TypeSizes.sizeof(hostId.getLeastSignificantBits());

        byte[] paramsBytes = JSONValue.toJSONString(parameters).getBytes(StandardCharsets.UTF_8);
        size += TypeSizes.sizeof(paramsBytes.length);
        size += 4; // size checksum
        size += paramsBytes.length;
        size += 4; // total checksum

        return size;
    }

    static HintsDescriptor deserialize(DataInput in) throws IOException
    {
        CRC32 crc = new CRC32();

        int version = in.readInt();
        updateChecksumInt(crc, version);

        long timestamp = in.readLong();
        updateChecksumLong(crc, timestamp);

        long msb = in.readLong();
        updateChecksumLong(crc, msb);
        long lsb = in.readLong();
        updateChecksumLong(crc, lsb);

        UUID hostId = new UUID(msb, lsb);

        int paramsLength = in.readInt();
        updateChecksumInt(crc, paramsLength);
        validateCRC(in.readInt(), (int) crc.getValue());

        byte[] paramsBytes = new byte[paramsLength];
        in.readFully(paramsBytes, 0, paramsLength);
        crc.update(paramsBytes, 0, paramsLength);
        validateCRC(in.readInt(), (int) crc.getValue());

        return new HintsDescriptor(hostId, version, timestamp, decodeJSONBytes(paramsBytes));
    }

    @SuppressWarnings("unchecked")
    private static ImmutableMap<String, Object> decodeJSONBytes(byte[] bytes)
    {
        return ImmutableMap.copyOf((Map<String, Object>) JSONValue.parse(new String(bytes, StandardCharsets.UTF_8)));
    }

    private static void updateChecksumLong(CRC32 crc, long value)
    {
        updateChecksumInt(crc, (int) (value & 0xFFFFFFFFL));
        updateChecksumInt(crc, (int) (value >>> 32));
    }

    private static void validateCRC(int expected, int actual) throws IOException
    {
        if (expected != actual)
            throw new ChecksumMismatchException("Hints Descriptor CRC Mismatch");
    }
}
