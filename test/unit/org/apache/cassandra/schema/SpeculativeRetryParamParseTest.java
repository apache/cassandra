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

import java.util.Arrays;
import java.util.Collection;

import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.exceptions.ConfigurationException;

import static org.junit.Assert.assertEquals;
import static org.junit.runners.Parameterized.Parameters;

@RunWith(Enclosed.class)
public class SpeculativeRetryParamParseTest
{

    @RunWith(Parameterized.class)
    public static class SuccessfulParseTest
    {
        private final String string;
        private final SpeculativeRetryParam expectedValue;

        public SuccessfulParseTest(String string, SpeculativeRetryParam expectedValue)
        {
            this.string = string;
            this.expectedValue = expectedValue;
        }

        @Parameters
        public static Collection<Object[]> generateData()
        {
            return Arrays.asList(new Object[][]{
                                 { "NONE", SpeculativeRetryParam.none() },
                                 { "ALWAYS", SpeculativeRetryParam.always() },
                                 { "10PERCENTILE", SpeculativeRetryParam.percentile(10.0) },
                                 { "121.1ms", SpeculativeRetryParam.custom(121.1) },
                                 { "21.7MS", SpeculativeRetryParam.custom(21.7) },
                                 { "None", SpeculativeRetryParam.none() },
                                 { "Always", SpeculativeRetryParam.always() },
                                 { "21.1percentile", SpeculativeRetryParam.percentile(21.1) },
                                 { "78.11p", SpeculativeRetryParam.percentile(78.11) }
                                 }
            );
        }

        @Test
        public void testParameterParse()
        {
            assertEquals(expectedValue, SpeculativeRetryParam.fromString(string));
        }
    }

    @RunWith(Parameterized.class)
    public static class FailedParseTest
    {
        private final String string;

        public FailedParseTest(String string)
        {
            this.string = string;
        }

        @Parameters
        public static Collection<Object[]> generateData()
        {
            return Arrays.asList(new Object[][]{
                                 { "" },
                                 { "-0.1PERCENTILE" },
                                 { "100.1PERCENTILE" },
                                 { "xPERCENTILE" },
                                 { "xyzms" },
                                 { "X" }
                                 }
            );
        }

        @Test(expected = ConfigurationException.class)
        public void testParameterParse()
        {
            SpeculativeRetryParam.fromString(string);
        }
    }
}