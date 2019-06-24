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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility method to retrieve information about the JRE.
 */
public final class JavaUtils
{
    private static final Logger logger = LoggerFactory.getLogger(JavaUtils.class);

    /**
     * Checks if the specified JRE support ExitOnOutOfMemory and CrashOnOutOfMemory.
     * @param jreVersion the JRE version
     * @return {@code true} if the running JRE support ExitOnOutOfMemory and CrashOnOutOfMemory or if the exact version
     * cannot be determined, {@code false} otherwise.
     */
    public static boolean supportExitOnOutOfMemory(String jreVersion)
    {
        try
        {
            int version = parseJavaVersion(jreVersion);

            if (version > 8)
                return true;

            int update = parseUpdateForPre9Versions(jreVersion);
            // The ExitOnOutOfMemory and CrashOnOutOfMemory are supported since the version 7u101 and 8u92
            return (version == 7 && update >= 101) || (version == 8 && update >= 92);
        }
        catch (Exception e)
        {
            logger.error("Some JRE information could not be retrieved for the JRE version: " + jreVersion, e);
            // We will continue assuming that the version supports ExitOnOutOfMemory and CrashOnOutOfMemory.
            return true;
        }
    }

    /**
     * Parses an Oracle JRE Version to extract the java version number.
     * <p> The parsing rules are based on the following
     * <a href='http://www.oracle.com/technetwork/java/javase/versioning-naming-139433.html'>String Naming Convention</a> and
     * <a href='http://openjdk.java.net/jeps/223'>JEP 223: New Version-String Scheme</a>.</p>
     * @param jreVersion the Oracle JRE Version
     * @return the java version number
     * @throws NumberFormatException if the version cannot be retrieved
     */
    public static int parseJavaVersion(String jreVersion)
    {
        String version;
        if (jreVersion.startsWith("1."))
        {
            version = jreVersion.substring(2, 3); // Pre 9 version
        }
        else
        {
            // Version > = 9
            int index = jreVersion.indexOf('.');

            if (index < 0)
            {
                // Does not have a minor version so we need to check for EA release
                index = jreVersion.indexOf('-');
                if (index < 0)
                    index = jreVersion.length();
            }
            version = jreVersion.substring(0, index);
        }
        return Integer.parseInt(version);
    }

    /**
     * Parses an Oracle JRE Version &lt; 9 to extract the update version.
     * <p> The parsing rules are based on the following
     * <a href='http://www.oracle.com/technetwork/java/javase/versioning-naming-139433.html'>String Naming Convention</a>.</p>
     * @param jreVersion the Oracle JRE Version
     * @return the update version
     * @throws NumberFormatException if the update cannot be retrieved
     */
    private static int parseUpdateForPre9Versions(String jreVersion)
    {
        // Handle non GA versions
        int dashSeparatorIndex = jreVersion.indexOf('-');
        if (dashSeparatorIndex > 0)
            jreVersion = jreVersion.substring(0, dashSeparatorIndex);

        int updateSeparatorIndex = jreVersion.indexOf('_');
        if (updateSeparatorIndex < 0)
            return 0; // Initial release

        return Integer.parseInt(jreVersion.substring(updateSeparatorIndex + 1));
    }

    private JavaUtils()
    {
    }
}
