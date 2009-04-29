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

import java.util.Collections;
import java.util.Set;

/**
 * Represents a record of metric data to be sent to a metrics system.
 *
 * Author : Avinash Lakshman ( alakshman@facebook.com) & Prashant Malik ( pmalik@facebook.com ) & Karthik Ranganathan ( kranganathan@facebook.com )
 */
public class OutputRecord
{
	private AnalyticsContext.TagMap tagMap;
	private AnalyticsContext.MetricMap metricMap;

	/**
	 * Creates a new instance of OutputRecord
	 */
	OutputRecord(AnalyticsContext.TagMap tagMap, AnalyticsContext.MetricMap metricMap)
	{
		this.tagMap = tagMap;
		this.metricMap = metricMap;
	}

	/**
	 * Returns the set of tag names.
	 */
	public Set<String> getTagNames()
	{
		return Collections.unmodifiableSet(tagMap.keySet());
	}

	/**
	 * Returns a tag object which is can be a String, Integer, Short or Byte.
	 *
	 * @return the tag value, or null if there is no such tag
	 */
	public Object getTag(String name)
	{
		return tagMap.get(name);
	}

	/**
	 * Returns the set of metric names.
	 *
	 * @return the set of metric names
	 */
	public Set<String> getMetricNames()
	{
		return Collections.unmodifiableSet(metricMap.keySet());
	}

	/**
	 * Returns the metric object which can be a Float, Integer, Short or Byte.
	 *
	 * @param name name of the metric for which the value is being requested
	 * @return return the tag value, or null if there is no such tag
	 */
	public Number getMetric(String name)
	{
		return (Number) metricMap.get(name);
	}

}