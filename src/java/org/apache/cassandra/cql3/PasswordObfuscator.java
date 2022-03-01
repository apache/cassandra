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

        // Regular expression:
        //  - Match new line and case insensitive (?si), and PASSWORD_TOKEN with greedy mode up to the start of the actual password and group it.
        //  - Quote the password between \Q and \E so any potential special characters are ignored
        //  - Replace the match with the grouped data + the obfuscated token
        return query.replaceAll("((?si)"+ PASSWORD_TOKEN + ".+?)\\Q" + pass.get() + "\\E",
                                "$1" + PasswordObfuscator.OBFUSCATION_TOKEN);
    }
}
