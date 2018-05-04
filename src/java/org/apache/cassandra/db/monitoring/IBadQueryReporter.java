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
package org.apache.cassandra.db.monitoring;

/**
 * Bad query reporter. All the badqueries happening currently will be reported here.
 * User will have to provide specific implementation about how to consume them.
 * By default log file implementation will be provided in which few of the badqueries will be
 * logged in sys log file. But one can override this interface and consume bad queries however they want
 * e.g. log in some different file, ELK, some form or in-memory SSTable implementation, etc.
 * Idea behind this reporting is user should be able to view bad queries easily, in real time and with enough
 * information so that it can be fixed at right place
 */
public interface IBadQueryReporter
{
    /**
     * setup badquery reporter.
     */
    void setup();

    /**
     * report bad query to reporter.
     *
     * @param type      badquery category.
     * @param operation operation category.
     */
    void reportBadQuery(BadQuery.BadQueryCategory type, final BadQueryTypes operation);
}
