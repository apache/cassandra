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


import java.io.Serializable;
import java.util.List;

//IC see: https://issues.apache.org/jira/browse/CASSANDRA-12564
abstract class Option implements Serializable
{

//IC see: https://issues.apache.org/jira/browse/CASSANDRA-6199
    abstract boolean accept(String param);
    abstract boolean happy();
    abstract String shortDisplay();
    abstract String longDisplay();
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-11914
    abstract String getOptionAsString(); // short and longDisplay print help text getOptionAsString prints value
    abstract List<String> multiLineDisplay();
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-6835
    abstract boolean setByUser();
    abstract boolean present();
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-8769

    public int hashCode()
    {
        return getClass().hashCode();
    }

    public boolean equals(Object that)
    {
        return this.getClass() == that.getClass();
    }

}
