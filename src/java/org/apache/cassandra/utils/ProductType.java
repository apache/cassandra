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

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProductType
{
    private static final Logger logger = LoggerFactory.getLogger(ProductType.class);

    public static Product product = getProduct();

    public enum Product
    {
        /**
         * On-Premises product
         */
        DATASTAX_CASSANDRA,

        /**
         * Datastax constellation database-as-a-service product (NOT referring to dse-db, aka. apollo)
         */
        DATASTAX_APOLLO
    }

    @VisibleForTesting
    public static Product getProduct()
    {
        Product defaultType = Product.DATASTAX_CASSANDRA;
        String productType = System.getProperty("dse.product_type", defaultType.name());
        try
        {
            return Product.valueOf(productType.toUpperCase());
        }
        catch (IllegalArgumentException e)
        {
            logger.info("Unknown product type '{}', will use default product type '{}'.", productType, defaultType.name());
            return defaultType;
        }
    }
}
