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

package org.apache.cassandra.db.view;

/**
 * ViewKeyRebuildConfig is configuration to control whether to rebuild the MV key on MV deletion.
 */
public class ViewKeyRebuildConfig
{
    /**
     * If true, the server will only accept MV deletions and treat it as a signal to rebuild the MV key.
     */
    public boolean rebuild_on_deletion_enabled = false;

    /**
     * If true, apply mutations to MV keys. Otherwise only print out the mutations that would be applied.
     */
    public boolean apply_mutations_enabled = false;

    /**
     * If true, logging more verbosely for debugging purpose.
     */
    public boolean verbose_logging_enabled = false;

    /**
     * If true, the server will do "strict" rebuild with read to both base and view table.
     * If false, the server will do "best-effort" to rebuild the MV key without reading from the MV.
     * When reading from base table only (set to false):
     * 1. May lead to duplicate MV entries on rewrite/delete action
     * 2. Will not have the latest MV row data in the log
     */
    public boolean view_read_enabled = false;

    // TODO: role name based access control, i.e., only allow certain users to perform the rebuild
}
