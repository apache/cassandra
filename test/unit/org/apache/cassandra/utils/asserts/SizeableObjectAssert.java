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

package org.apache.cassandra.utils.asserts;

import org.apache.cassandra.utils.ObjectSizes;

import static org.assertj.core.api.Assertions.assertThat;

public interface SizeableObjectAssert<SELF extends SizeableObjectAssert<SELF>>
{
    Object actual();

    default SELF hasSizeLessThan(double expectedSize)
    {
        double measured = (double) ObjectSizes.measureDeep(actual());
        assertThat(measured)
                  .withFailMessage("Size of measured object [%f] is not less than the expected size [%f]", measured, expectedSize)
                  .isLessThan(expectedSize);
        return ((SELF) this);

    }

    default SELF hasSizeGreaterThanOrEqual(double expectedSize)
    {
        double measured = (double) ObjectSizes.measureDeep(actual());
        assertThat(measured)
                  .withFailMessage("Size of measured object [%f] is not greater than or equal to the expected size [%f]", measured, expectedSize)
                  .isGreaterThanOrEqualTo(expectedSize);
        return ((SELF) this);
    }
}
