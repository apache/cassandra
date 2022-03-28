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

package org.apache.cassandra.service;

import org.junit.Before;

import static org.apache.cassandra.config.StartupChecksOptions.ENABLED_PROPERTY;
import static org.apache.cassandra.service.StartupChecks.StartupCheckType.check_filesystem_ownership;

public class YamlBasedFileSystemOwnershipCheckTest extends AbstractFilesystemOwnershipCheckTest
{
    @Before
    public void setup()
    {
        super.setup();
        options.getConfig(check_filesystem_ownership).put(ENABLED_PROPERTY, "true");
        options.getConfig(check_filesystem_ownership).put("ownership_token", token);
    }
}
