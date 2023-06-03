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

package org.apache.cassandra.cql3.functions.masking;

import org.junit.Assert;
import org.junit.Test;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ResultSet;
import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.db.marshal.StringType;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.schema.SchemaConstants;
import org.assertj.core.api.Assertions;

import static java.lang.String.format;

/**
 * Tests for {@link PartialMaskingFunction}.
 */
public class PartialMaskingFunctionTest extends MaskingFunctionTester
{
    @Override
    protected void testMaskingOnColumn(String name, CQL3Type type, Object value) throws Throwable
    {
        testMaskingOnColumn(PartialMaskingFunction.Kind.INNER, name, type, value);
        testMaskingOnColumn(PartialMaskingFunction.Kind.OUTER, name, type, value);
    }

    protected void testMaskingOnColumn(PartialMaskingFunction.Kind masker, String name, CQL3Type type, Object value) throws Throwable
    {
        String functionName = SchemaConstants.SYSTEM_KEYSPACE_NAME + ".mask_" + masker.name().toLowerCase();

        if (type.getType() instanceof StringType)
        {
            StringType stringType = (StringType) type.getType();
            String stringValue = (String) value;

            // ... with default padding
            assertRows(execute(format("SELECT %s(%s, 1, 2) FROM %%s", functionName, name)),
                       row(masker.mask(stringValue, 1, 2, PartialMaskingFunction.DEFAULT_PADDING_CHAR)));

            // ... with manually specified ASCII padding
            assertRows(execute(format("SELECT %s(%s, 1, 2, '#') FROM %%s", functionName, name)),
                       row(masker.mask(stringValue, 1, 2, '#')));

            // ... with manually specified UTF-8 padding
            assertRows(execute(format("SELECT %s((text) %s, 1, 2, 'é') FROM %%s", functionName, name)),
                       row(masker.mask(stringValue, 1, 2, 'é')));

            // ... with not single-character padding
            assertInvalidThrowMessage("should be single-character",
                                      InvalidRequestException.class,
                                      format("SELECT %s(%s, 1, 2, 'too_long') FROM %%s", functionName, name));

            // ... with null padding
            assertRows(execute(format("SELECT %s(%s, 1, 2, null) FROM %%s", functionName, name)),
                       row(masker.mask(stringValue, 1, 2, PartialMaskingFunction.DEFAULT_PADDING_CHAR)));

            // ... with null begin
            assertRows(execute(format("SELECT %s(%s, null, 2) FROM %%s", functionName, name)),
                       row(masker.mask(stringValue, 0, 2, PartialMaskingFunction.DEFAULT_PADDING_CHAR)));

            // ... with null end
            assertRows(execute(format("SELECT %s(%s, 1, null) FROM %%s", functionName, name)),
                       row(masker.mask(stringValue, 1, 0, PartialMaskingFunction.DEFAULT_PADDING_CHAR)));

            // test result set metadata, it should always be of type text, regardless of the type of the column
            ResultSet rs = executeNet(format("SELECT %s(%s, 1, 2) FROM %%s", functionName, name));
            ColumnDefinitions definitions = rs.getColumnDefinitions();
            Assert.assertEquals(1, definitions.size());
            Assert.assertEquals(driverDataType(stringType), definitions.getType(0));
            Assert.assertEquals(format("%s(%s, 1, 2)", functionName, name), definitions.getName(0));
        }
        else
        {
            assertInvalidThrowMessage(format("Function %s requires an argument of type [text|varchar|ascii], " +
                                             "but found argument %s of type %s",
                                             functionName, name, type),
                                      InvalidRequestException.class,
                                      format("SELECT %s(%s, 1, 2) FROM %%s", functionName, name));
        }
    }

    private static DataType driverDataType(StringType type)
    {
        switch ((CQL3Type.Native) type.asCQL3Type())
        {
            case ASCII:
                return DataType.ascii();
            case VARCHAR:
                return DataType.varchar();
            case TEXT:
                return DataType.text();
            default:
                throw new AssertionError();
        }
    }

    @Test
    public void testMasking()
    {
        // null value
        testMasking(null, 0, 0, '*', null, null);
        testMasking(null, 9, 9, '*', null, null);
        testMasking(null, 0, 0, '#', null, null);
        testMasking(null, 9, 9, '#', null, null);

        // empty value
        testMasking("", 0, 0, '*', "", "");
        testMasking("", 9, 9, '*', "", "");
        testMasking("", 0, 0, '#', "", "");
        testMasking("", 9, 9, '#', "", "");

        // single-char value
        testMasking("a", 0, 0, '*', "*", "a");
        testMasking("a", 1, 1, '*', "a", "*");
        testMasking("a", 10, 10, '*', "a", "*");
        testMasking("a", 0, 0, '#', "#", "a");
        testMasking("a", 1, 1, '#', "a", "#");
        testMasking("a", 10, 10, '#', "a", "#");

        // regular value
        testMasking("abcde", 0, 0, '*', "*****", "abcde");
        testMasking("abcde", 0, 1, '*', "****e", "abcd*");
        testMasking("abcde", 0, 2, '*', "***de", "abc**");
        testMasking("abcde", 0, 3, '*', "**cde", "ab***");
        testMasking("abcde", 0, 4, '*', "*bcde", "a****");
        testMasking("abcde", 0, 5, '*', "abcde", "*****");
        testMasking("abcde", 0, 6, '*', "abcde", "*****");
        testMasking("abcde", 1, 0, '*', "a****", "*bcde");
        testMasking("abcde", 2, 0, '*', "ab***", "**cde");
        testMasking("abcde", 3, 0, '*', "abc**", "***de");
        testMasking("abcde", 4, 0, '*', "abcd*", "****e");
        testMasking("abcde", 5, 0, '*', "abcde", "*****");
        testMasking("abcde", 6, 0, '*', "abcde", "*****");
        testMasking("abcde", 1, 1, '*', "a***e", "*bcd*");
        testMasking("abcde", 2, 2, '*', "ab*de", "**c**");
        testMasking("abcde", 3, 3, '*', "abcde", "*****");
        testMasking("abcde", 4, 4, '*', "abcde", "*****");

        // special characters
        testMasking("á#íòü", 0, 0, '*', "*****", "á#íòü");
        testMasking("á#íòü", 1, 1, '*', "á***ü", "*#íò*");
        testMasking("á#íòü", 5, 5, '*', "á#íòü", "*****");
    }

    private static void testMasking(String unmaskedValue,
                                    int begin,
                                    int end,
                                    char padding,
                                    String innerMaskedValue,
                                    String outerMaskedValue)
    {
        Assertions.assertThat(PartialMaskingFunction.Kind.INNER.mask(unmaskedValue, begin, end, padding))
                  .isIn(innerMaskedValue);
        Assertions.assertThat(PartialMaskingFunction.Kind.OUTER.mask(unmaskedValue, begin, end, padding))
                  .isIn(outerMaskedValue);
    }
}
