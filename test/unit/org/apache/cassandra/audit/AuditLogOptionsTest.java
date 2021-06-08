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

package org.apache.cassandra.audit;

import java.util.Collections;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.assertj.core.api.Assertions;

public class AuditLogOptionsTest
{
    @Test
    public void testAuditLogOptions()
    {
        AuditLogOptions defaultOptions = new AuditLogOptions();
        defaultOptions.enabled = false;
        defaultOptions.included_categories = "dcl, ddl";
        defaultOptions.included_keyspaces = "ks1, ks2";

        AuditLogOptions options = new AuditLogOptions.Builder(defaultOptions).withEnabled(true).build();
        Assert.assertEquals("DCL,DDL", options.included_categories);
        Assert.assertEquals("ks1,ks2", options.included_keyspaces);
        Assert.assertTrue(options.enabled);
        Assert.assertNotNull(options.audit_logs_dir);
        Assert.assertEquals(BinAuditLogger.class.getSimpleName(), options.logger.class_name);
        Assert.assertEquals(Collections.emptyMap(), options.logger.parameters);
    }

    @Test
    public void testInvalidCategoryShouldThrow()
    {
        Assertions.assertThatExceptionOfType(ConfigurationException.class)
                  .isThrownBy(() -> new AuditLogOptions.Builder()
                                    .withIncludedCategories("invalidCategoryName,dcl")
                                    .build());
    }
}
