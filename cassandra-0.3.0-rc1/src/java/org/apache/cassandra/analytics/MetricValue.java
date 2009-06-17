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

package org.apache.cassandra.analytics;


/**
 * A Number that is either an absolute or an incremental amount.
 *
 * Author : Avinash Lakshman ( alakshman@facebook.com) & Prashant Malik ( pmalik@facebook.com ) & Karthik Ranganathan ( kranganathan@facebook.com )
 */
public class MetricValue
{
	public static final boolean ABSOLUTE = false;
	public static final boolean INCREMENT = true;

	private boolean isIncrement;
	private Number number;

	/**
	 * Creates a new instance of MetricValue
	 *
	 *  @param number this initializes the initial value of this metric
	 *  @param isIncrement sets if the metric can be incremented or only set
	 */
	public MetricValue(Number number, boolean isIncrement)
	{
		this.number = number;
		this.isIncrement = isIncrement;
	}

	/**
	 * Checks if this metric can be incremented.
	 *
	 * @return true if the value of this metric can be incremented, false otherwise
	 */
	public boolean isIncrement()
	{
		return isIncrement;
	}

	/**
	 * Checks if the value of this metric is always an absolute value. This is the
	 * inverse of isIncrement.
	 *
	 * @return true if the
	 */
	public boolean isAbsolute()
	{
		return !isIncrement;
	}

	/**
	 * Returns the current number value of the metric.
	 *
	 * @return the Number value of this metric
	 */
	public Number getNumber()
	{
		return number;
	}
}
