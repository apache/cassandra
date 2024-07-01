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

package org.apache.cassandra.tools.nodetool.formatter;

import org.apache.cassandra.config.CassandraRelevantEnv;

/**
 * Create ANSI escape sequence to set foreground color for warning and alert messages.
 * 
 */
public enum OutputColor
{
    /**
     * Warning color is yellow by default or overwritten via environment variable NODETOOL_WARNING_COLOR
     */
    WARNING (33, CassandraRelevantEnv.NODETOOL_WARNING_COLOR.getString()),
    
    /**
     * Alert color is red by default or overwritten via environment variable NODETOOL_ALERT_COLOR
     */
    ALERT   (31, CassandraRelevantEnv.NODETOOL_ALERT_COLOR.getString()),
    
    /**
     * This generates a ANSI escape sequence to reset the foreground color.
     */
    RESET   (0, null);

    private final int code;
    
    OutputColor(int code, String override)
    {
        int altCode = code;
        try {
            altCode = Integer.decode(override);
            if (altCode < 30 || altCode > 37)
                throw new IllegalArgumentException();
        }
        catch (NullPointerException | IllegalArgumentException ex) {
            altCode = code;
        }
        finally {
            this.code = altCode;
        }
    }

    @Override
    public String toString()
    {
        return "\u001b[" + code + "m";
    }  
}
