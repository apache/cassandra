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

import org.junit.Assert;
import org.junit.Test;

public class EitherTest
{
    @Test
    public void left() {
        Either<String, Integer> e = Either.left("some string");

        Assert.assertTrue("should be a left but was not", e.isLeft());
        Assert.assertFalse("Should be a left but right is true", e.isRight());

        Assert.assertEquals("some string", e.toLeft().getValue());

        Either<Integer, Integer> mapped = e.map(String::length);

        Assert.assertTrue("should be a left but was not", mapped.isLeft());
        Assert.assertFalse("Should be a left but right is true", mapped.isRight());

        Assert.assertEquals("some string".length(), mapped.toLeft().getValue().intValue());
    }

    @Test
    public void right() {
        Either<String, Integer> e = Either.right(42);

        Assert.assertFalse("should be a right but left is true", e.isLeft());
        Assert.assertTrue("Should be a right but is not", e.isRight());

        Assert.assertEquals(42, e.toRight().getValue().intValue());

        // map is left bias, so does nothing
        Either<Integer, Integer> mapped = e.map(String::length);

        Assert.assertFalse("should be a right but was left", mapped.isLeft());
        Assert.assertTrue("Should be a right but was not", mapped.isRight());

        Assert.assertEquals(42, mapped.toRight().getValue().intValue());
    }
}