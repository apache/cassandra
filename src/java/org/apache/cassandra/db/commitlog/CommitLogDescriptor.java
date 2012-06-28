/*
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * 
 */
package org.apache.cassandra.db.commitlog;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.cassandra.net.MessagingService;

public class CommitLogDescriptor
{
    private static final String SEPARATOR = "-";
    private static final String FILENAME_PREFIX = "CommitLog" + SEPARATOR;
    private static final String FILENAME_EXTENSION = ".log";
    // match both legacy and new version of commitlogs Ex: CommitLog-12345.log and CommitLog-4-12345.log. 
    private static final Pattern COMMIT_LOG_FILE_PATTERN = Pattern.compile(FILENAME_PREFIX + "((\\d+)(" + SEPARATOR + "\\d+)?)" + FILENAME_EXTENSION);
    
    public static final int LEGACY_VERSION = 1;
    public static final int VERSION_12 = 2;
    /** 
     * Increment this number if there is a changes in the commit log disc layout or MessagingVersion changes.
     * Note: make sure to handle {@link #getMessagingVersion()}
     */
    public static final int current_version = VERSION_12;
    
    private final int version;
    public final long id;
    
    public CommitLogDescriptor(int version, long id)
    {
        this.version = version;
        this.id = id;
    }

    public CommitLogDescriptor(long id)
    {
        this(current_version, id);
    }

    public static CommitLogDescriptor fromFileName(String name)
    {
        Matcher matcher;
        if (!(matcher = COMMIT_LOG_FILE_PATTERN.matcher(name)).matches())
            throw new RuntimeException("Cannot parse the version of the file: " + name);

        if (matcher.group(3) != null)
        {
            long id = Long.valueOf(matcher.group(3).split(SEPARATOR)[1]);
            return new CommitLogDescriptor(Integer.valueOf(matcher.group(2)), id);
        }
        else
        {
            long id = Long.valueOf(matcher.group(1));
            return new CommitLogDescriptor(LEGACY_VERSION, id);
        }
    }

    public int getMessagingVersion()
    {
        assert MessagingService.current_version == MessagingService.VERSION_12;
        switch (version)
        {
            case LEGACY_VERSION:
                return MessagingService.VERSION_11;
            case VERSION_12:
                return MessagingService.VERSION_12;
            default:
                throw new IllegalStateException("Unknown commitlog version " + version);
        }
    }

    public String fileName()
    {   
        return FILENAME_PREFIX + version + SEPARATOR + id + FILENAME_EXTENSION;
    }

    /**
     * @param   filename  the filename to check
     * @return true if filename could be a commit log based on it's filename
     */
    public static boolean isValid(String filename)
    {
        return COMMIT_LOG_FILE_PATTERN.matcher(filename).matches();
    }
}
