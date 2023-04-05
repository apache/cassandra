package org.apache.cassandra.stress.settings;
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


public enum CqlVersion
{

    NOCQL(null),
    CQL3("3.0.0");

    public final String connectVersion;

    private CqlVersion(String connectVersion)
    {
        this.connectVersion = connectVersion;
    }

    static CqlVersion get(String version)
    {
        if (version == null)
            return NOCQL;
        switch(version.charAt(0))
        {
            case '3':
                return CQL3;
            default:
                throw new IllegalStateException();
        }
    }

    public boolean isCql()
    {
        return this != NOCQL;
    }

    public boolean isCql3()
    {
        return this == CQL3;
    }

}

