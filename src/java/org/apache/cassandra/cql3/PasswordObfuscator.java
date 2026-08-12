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

package org.apache.cassandra.cql3;

import java.util.Optional;
import java.util.regex.Pattern;

import org.apache.cassandra.auth.PasswordAuthenticator;
import org.apache.cassandra.auth.RoleOptions;

/**
 * Obfuscates passwords in a given string
 */
public class PasswordObfuscator
{
    public static final String OBFUSCATION_TOKEN = "*******";
    public static final String PASSWORD_TOKEN = PasswordAuthenticator.PASSWORD_KEY.toLowerCase();

    /**
     * Obfuscates everything after the first appearance password token
     * 
     * @param sourceString The query to obfuscate
     * @return The obfuscated query
     */
    public static String obfuscate(String sourceString)
    {
        if (null == sourceString)
            return null;

        int passwordTokenStartIndex = sourceString.toLowerCase().indexOf(PASSWORD_TOKEN);
        if (passwordTokenStartIndex < 0)
            return sourceString;

        return sourceString.substring(0, passwordTokenStartIndex + PASSWORD_TOKEN.length()) + " " + OBFUSCATION_TOKEN;
    }

    /**
     * Obfuscates the password in a query
     * 
     * @param query The query whose password to obfuscate
     * @param opts The options containing the password to obfuscate
     * @return The query with obfuscated password
     */
    public static String obfuscate(String query, RoleOptions opts)
    {
        if (opts == null || query == null || query.isEmpty())
            return query;

        Optional<String> pass = opts.getPassword();
        if (!pass.isPresent() || pass.get().isEmpty())
            pass = opts.getHashedPassword();
        if (!pass.isPresent() || pass.get().isEmpty())
            return query;

        // The password in RoleOptions is unescaped. Depending on the CQL string-literal syntax used
        // in the query it appears either single-quote-escaped (conventional '...' literals double any
        // embedded single quote) or verbatim (pg-style $$...$$ literals do not escape anything). Match
        // either form so both are obfuscated - matching only one leaks the password for the other
        // (e.g. CREATE ROLE r WITH PASSWORD = $$pa'ss$$). Pattern.quote() keeps regex metacharacters literal.
        String rawPassword = pass.get();
        String escapedPassword = rawPassword.replace("'", "''");

        String passwordForms = Pattern.quote(escapedPassword) + '|' + Pattern.quote(rawPassword);
        String pattern = "((?si)" + PASSWORD_TOKEN + ".+?)(?:" + passwordForms + ')';
        return query.replaceAll(pattern, "$1" + OBFUSCATION_TOKEN);
    }
}
