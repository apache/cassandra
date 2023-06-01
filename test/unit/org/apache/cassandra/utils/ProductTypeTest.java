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

package org.apache.cassandra.utils;

import org.junit.AfterClass;
import org.junit.Test;


import static org.junit.Assert.assertEquals;

public class ProductTypeTest
{
    @AfterClass
    public static void cleanup()
    {
        System.clearProperty("dse.product_type");
    }

    @Test
    public void testDefault()
    {
        System.clearProperty("dse.product_type");
        assertEquals(ProductType.Product.DATASTAX_CASSANDRA, ProductType.getProduct());
    }

    @Test
    public void testDatastaxApollo()
    {
        System.setProperty("dse.product_type", "DATASTAX_APOLLO");
        assertEquals(ProductType.Product.DATASTAX_APOLLO, ProductType.getProduct());

        System.setProperty("dse.product_type", "datastax_apollo");
        assertEquals(ProductType.Product.DATASTAX_APOLLO, ProductType.getProduct());
    }
}
