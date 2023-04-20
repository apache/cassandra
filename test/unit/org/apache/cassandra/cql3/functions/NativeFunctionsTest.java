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

package org.apache.cassandra.cql3.functions;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.junit.Test;

import org.apache.cassandra.schema.SchemaConstants;
import org.assertj.core.api.Assertions;

public class NativeFunctionsTest
{
    /**
     * Map associating old functions that don't satisfy the naming conventions adopted by CASSANDRA-18037 to their
     * new names according to those conventions.
     */
    private static final Map<String, String> LEGACY_FUNCTION_NAMES = new HashMap<String, String>()
    {
        {
            put("castAsAscii", "cast_as_ascii");
            put("castAsBigint", "cast_as_bigint");
            put("castAsDate", "cast_as_date");
            put("castAsDecimal", "cast_as_decimal");
            put("castAsDouble", "cast_as_double");
            put("castAsFloat", "cast_as_float");
            put("castAsInt", "cast_as_int");
            put("castAsSmallint", "cast_as_smallint");
            put("castAsText", "cast_as_text");
            put("castAsTimestamp", "cast_as_timestamp");
            put("castAsTinyint", "cast_as_tinyint");
            put("castAsVarint", "cast_as_varint");
            put("blobasascii", "blob_as_ascii");
            put("blobasbigint", "blob_as_bigint");
            put("blobasboolean", "blob_as_boolean");
            put("blobascounter", "blob_as_counter");
            put("blobasdate", "blob_as_date");
            put("blobasdecimal", "blob_as_decimal");
            put("blobasdouble", "blob_as_double");
            put("blobasduration", "blob_as_duration");
            put("blobasempty", "blob_as_empty");
            put("blobasfloat", "blob_as_float");
            put("blobasinet", "blob_as_inet");
            put("blobasint", "blob_as_int");
            put("blobassmallint", "blob_as_smallint");
            put("blobastext", "blob_as_text");
            put("blobastime", "blob_as_time");
            put("blobastimestamp", "blob_as_timestamp");
            put("blobastimeuuid", "blob_as_timeuuid");
            put("blobastinyint", "blob_as_tinyint");
            put("blobasuuid", "blob_as_uuid");
            put("blobasvarchar", "blob_as_varchar");
            put("blobasvarint", "blob_as_varint");
            put("asciiasblob", "ascii_as_blob");
            put("bigintasblob", "bigint_as_blob");
            put("booleanasblob", "boolean_as_blob");
            put("counterasblob", "counter_as_blob");
            put("dateasblob", "date_as_blob");
            put("decimalasblob", "decimal_as_blob");
            put("doubleasblob", "double_as_blob");
            put("durationasblob", "duration_as_blob");
            put("emptyasblob", "empty_as_blob");
            put("floatasblob", "float_as_blob");
            put("inetasblob", "inet_as_blob");
            put("intasblob", "int_as_blob");
            put("smallintasblob", "smallint_as_blob");
            put("textasblob", "text_as_blob");
            put("timeasblob", "time_as_blob");
            put("timestampasblob", "timestamp_as_blob");
            put("timeuuidasblob", "timeuuid_as_blob");
            put("tinyintasblob", "tinyint_as_blob");
            put("uuidasblob", "uuid_as_blob");
            put("varcharasblob", "varchar_as_blob");
            put("varintasblob", "varint_as_blob");
            put("countRows", "count_rows");
            put("maxtimeuuid", "max_timeuuid");
            put("mintimeuuid", "min_timeuuid");
            put("currentdate", "current_date");
            put("currenttime", "current_time");
            put("currenttimestamp", "current_timestamp");
            put("currenttimeuuid", "current_timeuuid");
            put("todate", "to_date");
            put("totimestamp", "to_timestamp");
            put("tounixtimestamp", "to_unix_timestamp");
        }
    };

    /**
     * Map associating old functions factories that don't satisfy the naming conventions adopted by CASSANDRA-18037 to
     * their new names according to those conventions.
     */
    private static final Map<String, String> LEGACY_FUNCTION_FACTORY_NAMES = new HashMap<String, String>()
    {
        {
            put("tojson", "to_json");
            put("fromjson", "from_json");
        }
    };

    /**
     * Verify that the old functions that don't satisfy the naming conventions adopted by CASSANDRA-18037 have a
     * replacement function that satisfies those conventions.
     */
    @Test
    public void testDeprectedFunctionNames()
    {
        NativeFunctions nativeFunctions = NativeFunctions.instance;
        LEGACY_FUNCTION_NAMES.forEach((oldName, newName) -> {
            Assertions.assertThat(nativeFunctions.getFunctions(FunctionName.nativeFunction(oldName))).isNotEmpty();
            Assertions.assertThat(nativeFunctions.getFunctions(FunctionName.nativeFunction(newName))).isNotEmpty();
        });

        for (NativeFunction function : nativeFunctions.getFunctions())
        {
            String name = function.name.name;

            if (satisfiesConventions(function.name))
                continue;

            Assertions.assertThat(LEGACY_FUNCTION_NAMES).containsKey(name);
            FunctionName newName = FunctionName.nativeFunction(LEGACY_FUNCTION_NAMES.get(name));

            Function newFunction = FunctionResolver.get(SchemaConstants.SYSTEM_KEYSPACE_NAME,
                                                        newName,
                                                        function.argTypes,
                                                        null,
                                                        null,
                                                        function.returnType);

            Assertions.assertThat(newFunction).isNotNull();
            Assertions.assertThat(function).isNotEqualTo(newFunction);
            Assertions.assertThat(function).isEqualTo(((NativeFunction) newFunction).withLegacyName());
            Assertions.assertThat(function.argTypes()).isEqualTo(newFunction.argTypes());
            Assertions.assertThat(function.returnType()).isEqualTo(newFunction.returnType());
            Assertions.assertThat(function.getClass()).isEqualTo(newFunction.getClass());
            Assertions.assertThat(function.name().name.toLowerCase())
                      .isEqualTo(StringUtils.remove(newFunction.name().name, '_'));
        }
    }

    /**
     * Verify that the old functions function factories that don't satisfy the naming conventions adopted by
     * CASSANDRA-18037 have a replacement function factory that satisfies those conventions.
     */
    @Test
    public void testDeprectedFunctionFactoryNames()
    {
        NativeFunctions nativeFunctions = NativeFunctions.instance;
        LEGACY_FUNCTION_FACTORY_NAMES.forEach((oldName, newName) -> {
            Assertions.assertThat(nativeFunctions.getFactories(FunctionName.nativeFunction(oldName))).isNotEmpty();
            Assertions.assertThat(nativeFunctions.getFactories(FunctionName.nativeFunction(newName))).isNotEmpty();
        });

        for (FunctionFactory factory : nativeFunctions.getFactories())
        {
            String name = factory.name.name;

            if (satisfiesConventions(factory.name))
                continue;

            Assertions.assertThat(LEGACY_FUNCTION_FACTORY_NAMES).containsKey(name);
            FunctionName newName = FunctionName.nativeFunction(LEGACY_FUNCTION_FACTORY_NAMES.get(name));
            Collection<FunctionFactory> newFactories = NativeFunctions.instance.getFactories(newName);

            Assertions.assertThat(newFactories).hasSize(1);
            FunctionFactory newFactory = newFactories.iterator().next();

            Assertions.assertThat(factory).isNotEqualTo(newFactory);
            Assertions.assertThat(factory.name).isNotEqualTo(newFactory.name);
            Assertions.assertThat(factory.parameters).isEqualTo(newFactory.parameters);
            Assertions.assertThat(factory.getClass()).isEqualTo(newFactory.getClass());
            Assertions.assertThat(factory.name().name.toLowerCase())
                      .isEqualTo(StringUtils.remove(newFactory.name().name, '_'));
        }
    }

    private static boolean satisfiesConventions(FunctionName functionName)
    {
        String name = functionName.name;
        return name.equals(name.toLowerCase()) &&
               !LEGACY_FUNCTION_NAMES.containsKey(name);
    }
}
