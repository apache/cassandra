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

package org.apache.cassandra.audit;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Obfuscates passwords in a given string
 */
class PasswordObfuscator implements IObfuscator
{
    private static final String OBFUSCATION_TOKEN = "*******";
    private static final String PASSWORD_TOKEN = "password";

    private static final int PATTERN_FLAGS = Pattern.CASE_INSENSITIVE | Pattern.DOTALL;
    private static final Pattern PASSWORD_PATTERN = Pattern.compile(".*password\\s*=?\\s*'(?<password>[^\\s]+)'.*",
                                                                    PATTERN_FLAGS);

    /**
     * Obfuscates passwords in DCL statements.
     *
     * @param sourceString string to obfuscate
     * @return obfuscated string, not containing passwords in plaintext
     */
    @Override
    public String obfuscate(String sourceString)
    {
        Matcher passwordMatcher = PASSWORD_PATTERN.matcher(sourceString);
        if (!passwordMatcher.matches())
        {
            return sourceString;
        }

        StringBuilder obfuscated = new StringBuilder();
        int matchStart = passwordMatcher.start(PASSWORD_TOKEN);
        int matchEnd = passwordMatcher.end(PASSWORD_TOKEN);
        obfuscated.append(sourceString, 0, matchStart).append(OBFUSCATION_TOKEN).append(sourceString.substring(matchEnd));
        return obfuscated.toString();
    }

    @Override
    public String getObfuscationToken()
    {
        return OBFUSCATION_TOKEN;
    }
}
