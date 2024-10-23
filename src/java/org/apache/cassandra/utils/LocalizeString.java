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

import java.util.Locale;

public class LocalizeString
{
    /**
     * Convert the String to lower case, using {@link java.util.Locale#US} by default
     */
    public static String toLowerCaseLocalized(String input)
    {
        return toLowerCaseLocalized(input, Locale.US);
    }

    /**
     * @param input  The string to be converted to lower case.
     * @param locale The locale to use for the conversion.
     * @return String itself with lowercase and localized by your selection of {@link java.util.Locale}.
     */
    public static String toLowerCaseLocalized(String input, Locale locale)
    {
        return input.toLowerCase(locale); // checkstyle: permit this invocation
    }

    /**
     * Convert the String to upper case, using {@link java.util.Locale#US} by default
     */
    public static String toUpperCaseLocalized(String input)
    {
        return toUpperCaseLocalized(input, Locale.US);
    }

    /**
     * @param input  The string to be converted to uppercase.
     * @param locale The locale to use for the conversion. This parameter is optional.
     * @return String itself with uppercase and localized by your selection of {@link java.util.Locale}.
     */
    public static String toUpperCaseLocalized(String input, Locale locale)
    {
        return input.toUpperCase(locale); // checkstyle: permit this invocation
    }
}