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

package org.apache.cassandra.config;

import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests for materialized view auto-backfill configuration.
 */
public class MaterializedViewAutoBackfillConfigTest
{
    @BeforeClass
    public static void setUpClass()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void testDefaultConfiguration()
    {
        // Test that the default configuration is correct
        Config config = new Config();
        assertTrue("Default auto-backfill setting should be true", 
                   config.materialized_view_auto_backfill_enabled);
    }

    @Test
    public void testConfigurationSetting()
    {
        // Test that we can set the configuration value
        Config config = new Config();
        
        config.materialized_view_auto_backfill_enabled = false;
        assertFalse("Should be able to set to false", 
                    config.materialized_view_auto_backfill_enabled);
        
        config.materialized_view_auto_backfill_enabled = true;
        assertTrue("Should be able to set to true", 
                   config.materialized_view_auto_backfill_enabled);
    }

    @Test
    public void testDatabaseDescriptorIntegration()
    {
        // Test that DatabaseDescriptor correctly reads from Config
        boolean originalSetting = DatabaseDescriptor.getMaterializedViewAutoBackfillEnabled();
        
        try {
            // Test setting to false
            DatabaseDescriptor.setMaterializedViewAutoBackfillEnabled(false);
            assertFalse("DatabaseDescriptor should return false when set to false",
                        DatabaseDescriptor.getMaterializedViewAutoBackfillEnabled());
            
            // Test setting to true
            DatabaseDescriptor.setMaterializedViewAutoBackfillEnabled(true);
            assertTrue("DatabaseDescriptor should return true when set to true",
                       DatabaseDescriptor.getMaterializedViewAutoBackfillEnabled());
            
        } finally {
            // Restore original setting
            DatabaseDescriptor.setMaterializedViewAutoBackfillEnabled(originalSetting);
        }
    }

    @Test
    public void testConfigurationPersistence()
    {
        // Test that configuration changes persist within the same JVM session
        boolean originalSetting = DatabaseDescriptor.getMaterializedViewAutoBackfillEnabled();
        
        try {
            // Change setting
            DatabaseDescriptor.setMaterializedViewAutoBackfillEnabled(false);
            
            // Verify it persists across multiple calls
            assertFalse("Setting should persist - call 1", 
                        DatabaseDescriptor.getMaterializedViewAutoBackfillEnabled());
            assertFalse("Setting should persist - call 2", 
                        DatabaseDescriptor.getMaterializedViewAutoBackfillEnabled());
            
            // Change back
            DatabaseDescriptor.setMaterializedViewAutoBackfillEnabled(true);
            
            // Verify new setting persists
            assertTrue("New setting should persist - call 1", 
                       DatabaseDescriptor.getMaterializedViewAutoBackfillEnabled());
            assertTrue("New setting should persist - call 2", 
                       DatabaseDescriptor.getMaterializedViewAutoBackfillEnabled());
            
        } finally {
            // Restore original setting
            DatabaseDescriptor.setMaterializedViewAutoBackfillEnabled(originalSetting);
        }
    }

    @Test
    public void testConfigFieldExists()
    {
        // Test that the Config class has the expected field
        try {
            java.lang.reflect.Field field = Config.class.getDeclaredField("materialized_view_auto_backfill_enabled");
            assertEquals("Field should be boolean type", boolean.class, field.getType());
            assertTrue("Field should be public", java.lang.reflect.Modifier.isPublic(field.getModifiers()));
            
        } catch (NoSuchFieldException e) {
            throw new AssertionError("Config class should have materialized_view_auto_backfill_enabled field", e);
        }
    }

    @Test
    public void testDatabaseDescriptorMethodsExist()
    {
        // Test that DatabaseDescriptor has the expected methods
        try {
            // Test getter method exists
            java.lang.reflect.Method getterMethod = DatabaseDescriptor.class.getDeclaredMethod("getMaterializedViewAutoBackfillEnabled");
            assertEquals("Getter should return boolean", boolean.class, getterMethod.getReturnType());
            assertTrue("Getter should be public", java.lang.reflect.Modifier.isPublic(getterMethod.getModifiers()));
            assertTrue("Getter should be static", java.lang.reflect.Modifier.isStatic(getterMethod.getModifiers()));
            
            // Test setter method exists
            java.lang.reflect.Method setterMethod = DatabaseDescriptor.class.getDeclaredMethod("setMaterializedViewAutoBackfillEnabled", boolean.class);
            assertEquals("Setter should return void", void.class, setterMethod.getReturnType());
            assertTrue("Setter should be public", java.lang.reflect.Modifier.isPublic(setterMethod.getModifiers()));
            assertTrue("Setter should be static", java.lang.reflect.Modifier.isStatic(setterMethod.getModifiers()));
            
        } catch (NoSuchMethodException e) {
            throw new AssertionError("DatabaseDescriptor should have required methods", e);
        }
    }

    @Test
    public void testConfigurationValidation()
    {
        // Test that the configuration accepts valid boolean values
        Config config = new Config();
        
        // These should all work without throwing exceptions
        config.materialized_view_auto_backfill_enabled = true;
        config.materialized_view_auto_backfill_enabled = false;
        
        // Verify the values are set correctly
        config.materialized_view_auto_backfill_enabled = true;
        assertTrue("Should accept true value", config.materialized_view_auto_backfill_enabled);
        
        config.materialized_view_auto_backfill_enabled = false;
        assertFalse("Should accept false value", config.materialized_view_auto_backfill_enabled);
    }
}
