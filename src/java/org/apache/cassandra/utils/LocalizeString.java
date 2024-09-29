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

/**
 * Converts core database jobs {@link String#toLowerCase()} and {@link String#toUpperCase()} operations.
 * Uses {@link java.util.Locale#US} to ensure consistency.
 * If no locale provided with second parameter in methods, {@link java.util.Locale#US} will be used as default.
 * If you need to do lowercase and uppercase for database operations this is required.
 * <strong>Do not use this function in simulators and stress tests.
 * It will not work. Just basic database operations for prevent locale conflict.</strong>
 * Otherwise, in other languages, there are conflicts in Java.
 * Also, probably not effective but this util has {@link NullPointerException} protection. Just in case.
 *
 * <p>See related JIRA issue (CASSANDRA-19953): <a href="https://issues.apache.org/jira/browse/CASSANDRA-19953">JIRA Issue</a></p>
 */
public class LocalizeString {

    private LocalizeString() {
        throw new IllegalStateException("Utility class");
    }

    /**
     * @param input The string to be converted to lowercase.
     * @return String itself lowercase and {@link java.util.Locale#US} implemented.
     */
    public static String toLowerCaseLocalized(String input) {
        if (input == null) return null; // Return it because prevent to NullPointerException
        return input.toLowerCase(Locale.US);
    }

    /**
     * @param input  The string to be converted to lowercase.
     * @param locale The locale to use for the conversion. This parameter is optional.
     *               Do not use this parameter if you are not using {@link java.util.Locale#ROOT} for Unicode jobs or {@link java.util.Locale#US} for date jobs etc.
     * @return String itself with lowercase and localized by your selection of {@link java.util.Locale}.
     */
    public static String toLowerCaseLocalized(String input, Locale locale) {
        if (input == null || locale == null) return null; // Return it because prevent to NullPointerException
        return input.toLowerCase(locale);
    }

    /**
     * @param input The string to be converted to uppercase.
     * @return String itself uppercase and {@link java.util.Locale#US} implemented.
     */
    public static String toUpperCaseLocalized(String input) {
        if (input == null) return null; // Return it because prevent to NullPointerException
        return input.toUpperCase(Locale.US);
    }

    /**
     * @param input  The string to be converted to uppercase.
     * @param locale The locale to use for the conversion. This parameter is optional.
     *               Do not use this parameter if you are not using {@link java.util.Locale#ROOT} for Unicode jobs or {@link java.util.Locale#ENGLISH} for date jobs etc.
     * @return String itself with uppercase and localized by your selection of {@link java.util.Locale}.
     */
    public static String toUpperCaseLocalized(String input, Locale locale) {
        if (input == null || locale == null) return null; // Return it because prevent to NullPointerException
        return input.toUpperCase(locale);
    }
}