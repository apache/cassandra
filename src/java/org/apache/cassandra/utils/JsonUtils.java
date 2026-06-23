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

package org.apache.cassandra.utils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.util.BufferRecyclers;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.datatype.jsr310.ser.InstantSerializer;

import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileInputStreamPlus;
import org.apache.cassandra.io.util.FileOutputStreamPlus;
import org.apache.cassandra.serializers.MarshalException;

import static org.apache.cassandra.io.util.File.WriteMode.OVERWRITE;
import static org.apache.cassandra.utils.LocalizeString.toLowerCaseLocalized;

public final class JsonUtils
{
    public static final ObjectMapper JSON_OBJECT_MAPPER = new ObjectMapper(new JsonFactory()); // checkstyle: permit this instantiation
    public static final ObjectWriter JSON_OBJECT_PRETTY_WRITER;

    private static class GlobalInstantSerializer extends InstantSerializer
    {
        private GlobalInstantSerializer()
        {
            super(InstantSerializer.INSTANCE,
                  false,
                  false,
                  DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").withZone(ZoneOffset.UTC));
        }
    }

    static
    {
        JSON_OBJECT_MAPPER.registerModule(new JavaTimeModule().addSerializer(Instant.class, new GlobalInstantSerializer()));
        JSON_OBJECT_PRETTY_WRITER = JSON_OBJECT_MAPPER.writerWithDefaultPrettyPrinter();
    }

    private JsonUtils()
    {
    }

    /**
     * Quotes string contents using standard JSON quoting.
     */
    public static String quoteAsJsonString(String s)
    {
        // In future should update to directly use `JsonStringEncoder.getInstance()` but for now:
        return new String(BufferRecyclers.getJsonStringEncoder().quoteAsString(s));
    }

    public static Object decodeJson(byte[] json)
    {
        try
        {
            return JSON_OBJECT_MAPPER.readValue(json, Object.class);
        }
        catch (IOException ex)
        {
            throw new MarshalException("Error decoding JSON bytes: " + ex.getMessage());
        }
    }

    public static Object decodeJson(String json)
    {
        try
        {
            return JSON_OBJECT_MAPPER.readValue(json, Object.class);
        }
        catch (IOException ex)
        {
            throw new MarshalException("Error decoding JSON string: " + ex.getMessage());
        }
    }

    public static byte[] writeAsJsonBytes(Object value)
    {
        try
        {
            return JSON_OBJECT_MAPPER.writeValueAsBytes(value);
        }
        catch (IOException ex)
        {
            throw new MarshalException("Error writing as JSON: " + ex.getMessage());
        }
    }

    public static String writeAsJsonString(Object value)
    {
        try
        {
            return JSON_OBJECT_MAPPER.writeValueAsString(value);
        }
        catch (IOException ex)
        {
            throw new MarshalException("Error writing as JSON: " + ex.getMessage());
        }
    }

    public static String writeAsPrettyJsonString(Object value) throws MarshalException
    {
        try
        {
            return JSON_OBJECT_PRETTY_WRITER.writeValueAsString(value);
        }
        catch (IOException ex)
        {
            throw new MarshalException("Error writing as JSON: " + ex.getMessage());
        }
    }

    public static <T> Map<String, T> fromJsonMap(String json)
    {
        try
        {
            return JSON_OBJECT_MAPPER.readValue(json, Map.class);
        }
        catch (IOException ex)
        {
            throw new MarshalException("Error decoding JSON string: " + ex.getMessage());
        }
    }

    public static <T> Map<String, T> fromJsonMap(byte[] bytes)
    {
        try
        {
            return JSON_OBJECT_MAPPER.readValue(bytes, Map.class);
        }
        catch (IOException ex)
        {
            throw new MarshalException("Error decoding JSON: " + ex.getMessage());
        }
    }

    public static List<String> fromJsonList(byte[] bytes)
    {
        try
        {
            return JSON_OBJECT_MAPPER.readValue(bytes, List.class);
        }
        catch (IOException ex)
        {
            throw new MarshalException("Error decoding JSON: " + ex.getMessage());
        }
    }

    public static List<String> fromJsonList(String json)
    {
        try
        {
            return JSON_OBJECT_MAPPER.readValue(json, List.class);
        }
        catch (IOException ex)
        {
            throw new MarshalException("Error decoding JSON: " + ex.getMessage());
        }
    }

    public static void serializeToJsonFile(Object object, File outputFile) throws IOException
    {
        try (FileOutputStreamPlus out = outputFile.newOutputStream(OVERWRITE))
        {
            JSON_OBJECT_PRETTY_WRITER.writeValue((OutputStream) out, object);
        }
    }

    public static void serializeToJsonFileAtomic(Object object, File outputFile) throws IOException
    {
        // Try to write then perform atomic move so that file can't be corrupted
        // by process crash in the middle of the writing.
        File tempFile = new File(outputFile.path() + ".tmp");
        try
        {
            // Serialize to bytes first so we can flush and fsync before close.
            // Jackson's writeValue(OutputStream, ...) auto-closes the stream,
            // which would prevent us from calling sync() afterwards.
            byte[] data = JSON_OBJECT_PRETTY_WRITER.writeValueAsBytes(object);
            try (FileOutputStreamPlus out = tempFile.newOutputStream(OVERWRITE))
            {
                out.write(data);
                // Force data to disk before rename to ensure durability.
                // Without this, a crash after rename but before OS flushes to disk
                // can leave the file with zero-filled or corrupted blocks.
                out.sync();
            }
            tempFile.move(outputFile);
            // Fsync the parent directory to ensure the rename is durable.
            // Without this, a crash after rename can revert to the old directory entry.
            // See: https://transactional.blog/how-to-learn/disk-io
            SyncUtil.trySyncDir(outputFile.parent());
        }
        catch (IOException ex)
        {
            tempFile.deleteIfExists();
            throw ex;
        }
    }

    public static <T> T deserializeFromJsonFile(Class<T> tClass, File file) throws IOException
    {
        try (FileInputStreamPlus in = file.newInputStream())
        {
            return JSON_OBJECT_MAPPER.readValue((InputStream) in, tClass);
        }
    }

    public static <T> T deserializeFromJsonBytes(Class<T> tClass, byte[] bytes) throws IOException
    {
        return JSON_OBJECT_MAPPER.readValue(bytes, tClass);
    }

    /**
     * Handles unquoting and case-insensitivity in map keys.
     */
    public static void handleCaseSensitivity(Map<String, Object> valueMap)
    {
        for (String mapKey : new ArrayList<>(valueMap.keySet()))
        {
            // if it's surrounded by quotes, remove them and preserve the case
            if (mapKey.startsWith("\"") && mapKey.endsWith("\""))
            {
                valueMap.put(mapKey.substring(1, mapKey.length() - 1), valueMap.remove(mapKey));
                continue;
            }

            // otherwise, lowercase it if needed
            String lowered = toLowerCaseLocalized(mapKey);
            if (!mapKey.equals(lowered))
                valueMap.put(lowered, valueMap.remove(mapKey));
        }
    }

    public static String getJsonType(Class<?> type)
    {
        if (type == String.class)
            return "string";
        else if (type == int.class || type == Integer.class)
            return "integer";
        else if (type == long.class || type == Long.class)
            return "integer";
        else if (type == boolean.class || type == Boolean.class)
            return "boolean";
        else if (type == double.class || type == Double.class ||
                 type == float.class || type == Float.class)
            return "number";
        else if (type.isArray() || List.class.isAssignableFrom(type))
            return "array";
        else if (Map.class.isAssignableFrom(type))
            return "object";
        else if (type.isEnum())
            return "string";
        else
            return "string";
    }

    public static Object convertDefaultValue(String defaultValue, Class<?> type)
    {
        try
        {
            if (type == boolean.class || type == Boolean.class)
                return Boolean.parseBoolean(defaultValue);
            else if (type == int.class || type == Integer.class)
                return Integer.parseInt(defaultValue);
            else if (type == long.class || type == Long.class)
                return Long.parseLong(defaultValue);
            else if (type == double.class || type == Double.class)
                return Double.parseDouble(defaultValue);
            else if (type == float.class || type == Float.class)
                return Float.parseFloat(defaultValue);
            else
                return defaultValue;
        }
        catch (Exception e)
        {
            // Fall back to string default value if parsing fails.
            return defaultValue;
        }
    }
}
