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

/**
 * Obfuscates passwords in a given string
 */
public class PasswordObfuscator
{
    public static final String OBFUSCATION_TOKEN = " *******";
    private static final String PASSWORD_TOKEN = "password";

    public String obfuscate(String sourceString)
    {
        if (null == sourceString)
            return null;

        int passwordTokenStartIndex = sourceString.toLowerCase().indexOf(PASSWORD_TOKEN);
        if (passwordTokenStartIndex < 0)
            return sourceString;

        return sourceString.substring(0, passwordTokenStartIndex + PASSWORD_TOKEN.length()) + OBFUSCATION_TOKEN;
    }
}