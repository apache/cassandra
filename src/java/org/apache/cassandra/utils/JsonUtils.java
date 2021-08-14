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
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.util.BufferRecyclers;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileInputStreamPlus;
import org.apache.cassandra.io.util.FileOutputStreamPlus;
import org.apache.cassandra.serializers.MarshalException;

import static org.apache.cassandra.io.util.File.WriteMode.OVERWRITE;

public final class JsonUtils
{
    public static final ObjectMapper JSON_OBJECT_MAPPER = new ObjectMapper(new JsonFactory()); // checkstyle: permit this instantiation
    public static final ObjectWriter JSON_OBJECT_PRETTY_WRITER;

    static
    {
        JSON_OBJECT_MAPPER.registerModule(new JavaTimeModule());
        JSON_OBJECT_MAPPER.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
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

    public static <T> T deserializeFromJsonFile(Class<T> tClass, File file) throws IOException
    {
        try (FileInputStreamPlus in = file.newInputStream())
        {
            return JSON_OBJECT_MAPPER.readValue((InputStream) in, tClass);
        }
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
            String lowered = mapKey.toLowerCase(Locale.US);
            if (!mapKey.equals(lowered))
                valueMap.put(lowered, valueMap.remove(mapKey));
        }
    }
}
