/**
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

/**
 * Log4j configurations may change while the application is running, 
 * potentially invalidating a logger's appender(s).  This is a convinience
 * class to wrap logger calls so that a logger is always explicitly 
 * invoked.
 */


public class Log4jLogger {
    
    private String name_ = null;
    
    public Log4jLogger(String name){
        name_ = name;
    }
    
    public void debug(Object arg){ 
        LogUtil.getLogger(name_).debug(LogUtil.getTimestamp() + " - " + arg);
    }    
    public void info(Object arg){
        LogUtil.getLogger(name_).info(LogUtil.getTimestamp() + " - " + arg);
    }
    public void warn(Object arg){
        LogUtil.getLogger(name_).warn(LogUtil.getTimestamp() + " - " + arg);
    }
    public void error(Object arg){
        LogUtil.getLogger(name_).error(LogUtil.getTimestamp() + " - " + arg);
    }
    public void fatal(Object arg){
        LogUtil.getLogger(name_).fatal(LogUtil.getTimestamp() + " - " + arg);
    } 
}
