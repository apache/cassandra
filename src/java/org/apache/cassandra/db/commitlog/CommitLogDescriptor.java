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
package org.apache.cassandra.db.commitlog;

import java.io.DataInput;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;
import com.github.tjake.ICRC32;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.ParameterizedClass;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.CRC32Factory;
import org.json.simple.JSONValue;

public class CommitLogDescriptor
{
    private static final String SEPARATOR = "-";
    private static final String FILENAME_PREFIX = "CommitLog" + SEPARATOR;
    private static final String FILENAME_EXTENSION = ".log";
    // match both legacy and new version of commitlogs Ex: CommitLog-12345.log and CommitLog-4-12345.log.
    private static final Pattern COMMIT_LOG_FILE_PATTERN = Pattern.compile(FILENAME_PREFIX + "((\\d+)(" + SEPARATOR + "\\d+)?)" + FILENAME_EXTENSION);
    private static final String COMPRESSION_PARAMETERS_KEY = "compressionParameters";
    private static final String COMPRESSION_CLASS_KEY = "compressionClass";

    public static final int VERSION_12 = 2;
    public static final int VERSION_20 = 3;
    public static final int VERSION_21 = 4;
    public static final int VERSION_30 = 5;
    /**
     * Increment this number if there is a changes in the commit log disc layout or MessagingVersion changes.
     * Note: make sure to handle {@link #getMessagingVersion()}
     */
    @VisibleForTesting
    public static final int current_version = VERSION_30;

    final int version;
    public final long id;
    public final ParameterizedClass compression;

    public CommitLogDescriptor(int version, long id, ParameterizedClass compression)
    {
        this.version = version;
        this.id = id;
        this.compression = compression;
    }

    public CommitLogDescriptor(long id, ParameterizedClass compression)
    {
        this(current_version, id, compression);
    }

    public static void writeHeader(ByteBuffer out, CommitLogDescriptor descriptor)
    {
        ICRC32 crc = CRC32Factory.instance.create();
        out.putInt(descriptor.version);
        crc.updateInt(descriptor.version);
        out.putLong(descriptor.id);
        crc.updateInt((int) (descriptor.id & 0xFFFFFFFFL));
        crc.updateInt((int) (descriptor.id >>> 32));
        if (descriptor.version >= VERSION_30) {
            String parametersString = constructParametersString(descriptor);
            byte[] parametersBytes = parametersString.getBytes(StandardCharsets.UTF_8);
            if (parametersBytes.length != (((short) parametersBytes.length) & 0xFFFF))
                throw new ConfigurationException(String.format("Compression parameters too long, length %d cannot be above 65535.",
                                                               parametersBytes.length));
            out.putShort((short) parametersBytes.length);
            crc.updateInt(parametersBytes.length);
            out.put(parametersBytes);
            crc.update(parametersBytes, 0, parametersBytes.length);
        } else
            assert descriptor.compression == null;
        out.putInt(crc.getCrc());
    }

    private static String constructParametersString(CommitLogDescriptor descriptor)
    {
        Map<String, Object> params = new TreeMap<String, Object>();
        ParameterizedClass compression = descriptor.compression;
        if (compression != null)
        {
            params.put(COMPRESSION_PARAMETERS_KEY, compression.parameters);
            params.put(COMPRESSION_CLASS_KEY, compression.class_name);
        }
        return JSONValue.toJSONString(params);
    }

    public static CommitLogDescriptor fromHeader(File file)
    {
        try (RandomAccessFile raf = new RandomAccessFile(file, "r"))
        {
            assert raf.getFilePointer() == 0;
            return readHeader(raf);
        }
        catch (EOFException e)
        {
            throw new RuntimeException(e);
        }
        catch (IOException e)
        {
            throw new FSReadError(e, file);
        }
    }

    public static CommitLogDescriptor readHeader(DataInput input) throws IOException
    {
        ICRC32 checkcrc = CRC32Factory.instance.create();
        int version = input.readInt();
        checkcrc.updateInt(version);
        long id = input.readLong();
        checkcrc.updateInt((int) (id & 0xFFFFFFFFL));
        checkcrc.updateInt((int) (id >>> 32));
        int parametersLength = 0;
        if (version >= VERSION_30) {
            parametersLength = input.readShort() & 0xFFFF;
            checkcrc.updateInt(parametersLength);
        }
        // This should always succeed as parametersLength cannot be too long even for a
        // corrupt segment file.
        byte[] parametersBytes = new byte[parametersLength];
        input.readFully(parametersBytes);
        checkcrc.update(parametersBytes, 0, parametersBytes.length);
        int crc = input.readInt();
        if (crc == checkcrc.getCrc())
            return new CommitLogDescriptor(version, id,
                    parseCompression((Map<?, ?>) JSONValue.parse(new String(parametersBytes, StandardCharsets.UTF_8))));
        return null;
    }

    @SuppressWarnings("unchecked")
    private static ParameterizedClass parseCompression(Map<?, ?> params)
    {
        if (params == null)
            return null;
        String className = (String) params.get(COMPRESSION_CLASS_KEY);
        if (className == null)
            return null;

        Map<String, String> cparams = (Map<String, String>) params.get(COMPRESSION_PARAMETERS_KEY);
        return new ParameterizedClass(className, cparams);
    }

    public static CommitLogDescriptor fromFileName(String name)
    {
        Matcher matcher;
        if (!(matcher = COMMIT_LOG_FILE_PATTERN.matcher(name)).matches())
            throw new RuntimeException("Cannot parse the version of the file: " + name);

        if (matcher.group(3) == null)
            throw new UnsupportedOperationException("Commitlog segment is too old to open; upgrade to 1.2.5+ first");

        long id = Long.parseLong(matcher.group(3).split(SEPARATOR)[1]);
        return new CommitLogDescriptor(Integer.parseInt(matcher.group(2)), id, null);
    }

    public int getMessagingVersion()
    {
        switch (version)
        {
            case VERSION_12:
                return MessagingService.VERSION_12;
            case VERSION_20:
                return MessagingService.VERSION_20;
            case VERSION_21:
                return MessagingService.VERSION_21;
            case VERSION_30:
                return MessagingService.VERSION_30;
            default:
                throw new IllegalStateException("Unknown commitlog version " + version);
        }
    }

    public String fileName()
    {
        return FILENAME_PREFIX + version + SEPARATOR + id + FILENAME_EXTENSION;
    }

    /**
     * @param   filename  the filename to check
     * @return true if filename could be a commit log based on it's filename
     */
    public static boolean isValid(String filename)
    {
        return COMMIT_LOG_FILE_PATTERN.matcher(filename).matches();
    }

    public String toString()
    {
        return "(" + version + "," + id + (compression != null ? "," + compression : "") + ")";
    }

    public boolean equals(Object that)
    {
        return that instanceof CommitLogDescriptor && equals((CommitLogDescriptor) that);
    }

    public boolean equalsIgnoringCompression(CommitLogDescriptor that)
    {
        return this.version == that.version && this.id == that.id;
    }

    public boolean equals(CommitLogDescriptor that)
    {
        return equalsIgnoringCompression(that) && Objects.equal(this.compression, that.compression);
    }

}
