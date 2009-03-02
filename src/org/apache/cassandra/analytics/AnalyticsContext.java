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

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.TreeMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.service.IComponentShutdown;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.LogUtil;
import org.apache.log4j.Logger;



/**
 * Context for sending metrics to Ganglia. This class drives the entire metric collection process.
 *
 * Author : Avinash Lakshman ( alakshman@facebook.com) & Prashant Malik ( pmalik@facebook.com ) & Karthik Ranganathan ( kranganathan@facebook.com )
 */
public class AnalyticsContext implements IComponentShutdown
{
	private static Logger logger_ = Logger.getLogger(AnalyticsContext.class);

	private static final String PERIOD_PROPERTY = "period";
	private static final String SERVERS_PROPERTY = "servers";
	private static final String UNITS_PROPERTY = "units";
	private static final String SLOPE_PROPERTY = "slope";
	private static final String TMAX_PROPERTY = "tmax";
	private static final String DMAX_PROPERTY = "dmax";

	private static final String DEFAULT_UNITS = "";
	private static final String DEFAULT_SLOPE = "both";
	private static final int DEFAULT_TMAX = 60;
	private static final int DEFAULT_DMAX = 0;
	private static final int DEFAULT_PORT = 8649;
	private static final int BUFFER_SIZE = 1500;			 // as per libgmond.c

	private static final Map<Class,String> typeTable_ = new HashMap<Class,String>(5);

	private Map<String,RecordMap> bufferedData_ = new HashMap<String,RecordMap>();
    /* Keeps the MetricRecord for each abstraction that implements IAnalyticsSource */
    private Map<String, MetricsRecord> recordMap_ = new HashMap<String, MetricsRecord>();
	private Map<String,Object> attributeMap_ = new HashMap<String,Object>();
	private Set<IAnalyticsSource> updaters = new HashSet<IAnalyticsSource>(1);
	private List<InetSocketAddress> metricsServers_;

	private Map<String, String> unitsTable_;
	private Map<String, String> slopeTable_;
	private Map<String, String> tmaxTable_;
	private Map<String, String> dmaxTable_;

	/* singleton instance */
	private static AnalyticsContext instance_;
    /* Used to lock the factory for creation of StorageService instance */
    private static Lock createLock_ = new ReentrantLock();

	/**
	 * Default period in seconds at which data is sent to the metrics system.
	*/
	private static final int DEFAULT_PERIOD = 5;

	/**
	 * Port to which we should write the data.
	 */
	private int port_ = DEFAULT_PORT;

	private Timer timer = null;
	private int period_ = DEFAULT_PERIOD;
	private volatile boolean isMonitoring = false;
	private byte[] buffer_ = new byte[BUFFER_SIZE];
	private int offset_;

	private DatagramSocket datagramSocket_;

	static class TagMap extends TreeMap<String,Object>
	{
		private static final long serialVersionUID = 3546309335061952993L;
		TagMap()
		{
			super();
		}
		TagMap(TagMap orig)
		{
			super(orig);
		}
	}

	static class MetricMap extends TreeMap<String,Number>
	{
		private static final long serialVersionUID = -7495051861141631609L;
	}

	static class RecordMap extends HashMap<TagMap,MetricMap>
	{
		private static final long serialVersionUID = 259835619700264611L;
	}

	static
	{
		typeTable_.put(String.class, "string");
		typeTable_.put(Byte.class, "int8");
		typeTable_.put(Short.class, "int16");
		typeTable_.put(Integer.class, "int32");
		typeTable_.put(Float.class, "float");
	}


	/**
	 * Creates a new instance of AnalyticsReporter
	 */
	public AnalyticsContext()
	{
		StorageService.instance().registerComponentForShutdown(this);
	}

	/**
	* Initializes the context.
	*/
	public void init(String contextName, String serverSpecList)
	{
		String periodStr = getAttribute(PERIOD_PROPERTY);

		if (periodStr != null)
		{
			int period = 0;
			try
			{
				period = Integer.parseInt(periodStr);
			}
			catch (NumberFormatException nfe)
			{
			}

			if (period <= 0)
			{
				throw new AnalyticsException("Invalid period: " + periodStr);
			}

			setPeriod(period);
		}

		metricsServers_ = parse(serverSpecList, port_);
		unitsTable_ = getAttributeTable(UNITS_PROPERTY);
		slopeTable_ = getAttributeTable(SLOPE_PROPERTY);
		tmaxTable_ = getAttributeTable(TMAX_PROPERTY);
		dmaxTable_ = getAttributeTable(DMAX_PROPERTY);

		try
		{
			datagramSocket_ = new DatagramSocket();
		}
		catch (SocketException se)
		{
			se.printStackTrace();
		}
	}

	/**
	 * Sends a record to the metrics system.
	 */
	public void emitRecord(String recordName, OutputRecord outRec) throws IOException
	{
		// emit each metric in turn
		for (String metricName : outRec.getMetricNames())
		{
			Object metric = outRec.getMetric(metricName);
			String type = (String) typeTable_.get(metric.getClass());
			emitMetric(metricName, type, metric.toString());
		}
	}

	/**
	 * Helper which actually writes the metric in XDR format.
	 *
	 * @param name
	 * @param type
	 * @param value
	 * @throws IOException
	 */
	private void emitMetric(String name, String type, String value) throws IOException
	{
		String units = getUnits(name);
		int slope = getSlope(name);
		int tmax = getTmax(name);
		int dmax = getDmax(name);
		offset_ = 0;

		xdr_int(0); // metric_user_defined
		xdr_string(type);
		xdr_string(name);
		xdr_string(value);
		xdr_string(units);
		xdr_int(slope);
		xdr_int(tmax);
		xdr_int(dmax);

		for (InetSocketAddress socketAddress : metricsServers_)
		{
			DatagramPacket packet = new DatagramPacket(buffer_, offset_, socketAddress);
			datagramSocket_.send(packet);
		}
	}

	private String getUnits(String metricName)
	{
		String result = (String) unitsTable_.get(metricName);
		if (result == null)
		{
			result = DEFAULT_UNITS;
		}

		return result;
	}

	private int getSlope(String metricName)
	{
		String slopeString = (String) slopeTable_.get(metricName);
		if (slopeString == null)
		{
			slopeString = DEFAULT_SLOPE;
		}

		return ("zero".equals(slopeString) ? 0 : 3); // see gmetric.c
	}

	private int getTmax(String metricName)
	{
		String tmaxString = (String) tmaxTable_.get(metricName);
		if (tmaxString == null)
		{
			return DEFAULT_TMAX;
		}
		else
		{
			return Integer.parseInt(tmaxString);
		}
	}

	private int getDmax(String metricName)
	{
		String dmaxString = (String) dmaxTable_.get(metricName);
		if (dmaxString == null)
		{
			return DEFAULT_DMAX;
		}
		else
		{
			return Integer.parseInt(dmaxString);
		}
	}

	/**
	 * Puts a string into the buffer by first writing the size of the string
	 * as an int, followed by the bytes of the string, padded if necessary to
	 * a multiple of 4.
	 */
	private void xdr_string(String s)
	{
		byte[] bytes = s.getBytes();
		int len = bytes.length;
		xdr_int(len);
		System.arraycopy(bytes, 0, buffer_, offset_, len);
		offset_ += len;
		pad();
	}

	/**
	 * Pads the buffer with zero bytes up to the nearest multiple of 4.
	 */
	private void pad()
	{
		int newOffset = ((offset_ + 3) / 4) * 4;
		while (offset_ < newOffset)
		{
			buffer_[offset_++] = 0;
		}
	}

	/**
	 * Puts an integer into the buffer as 4 bytes, big-endian.
	 */
	private void xdr_int(int i)
	{
		buffer_[offset_++] = (byte) ((i >> 24) & 0xff);
		buffer_[offset_++] = (byte) ((i >> 16) & 0xff);
		buffer_[offset_++] = (byte) ((i >> 8) & 0xff);
		buffer_[offset_++] = (byte) (i & 0xff);
	}



	/**
	 * Returns the names of all the factory's attributes.
	 *
	 * @return the attribute names
	 */
	public String[] getAttributeNames()
	{
		String[] result = new String[attributeMap_.size()];
		int i = 0;
		// for (String attributeName : attributeMap.keySet()) {
		Iterator<String> it = attributeMap_.keySet().iterator();
		while (it.hasNext())
		{
			result[i++] = it.next();
		}
		return result;
	}

	/**
	 * Sets the named factory attribute to the specified value, creating it
	 * if it did not already exist.	If the value is null, this is the same as
	 * calling removeAttribute.
	 *
	 * @param attributeName the attribute name
	 * @param value the new attribute value
	 */
	public void setAttribute(String attributeName, Object value)
	{
		attributeMap_.put(attributeName, value);
	}

	/**
	 * Removes the named attribute if it exists.
	 *
	 * @param attributeName the attribute name
	 */
	public void removeAttribute(String attributeName)
	{
		attributeMap_.remove(attributeName);
	}

	/**
	 * Returns the value of the named attribute, or null if there is no
	 * attribute of that name.
	 *
	 * @param attributeName the attribute name
	 * @return the attribute value
	 */
	public String getAttribute(String attributeName)
	{
		return (String)attributeMap_.get(attributeName);
	}


	/**
	 * Returns an attribute-value map derived from the factory attributes
	 * by finding all factory attributes that begin with
	 * <i>contextName</i>.<i>tableName</i>.	The returned map consists of
	 * those attributes with the contextName and tableName stripped off.
	 */
	protected Map<String,String> getAttributeTable(String tableName)
	{
		String prefix = tableName + ".";
		Map<String,String> result = new HashMap<String,String>();
		for (String attributeName : getAttributeNames())
		{
			if (attributeName.startsWith(prefix))
			{
				String name = attributeName.substring(prefix.length());
				String value = (String) getAttribute(attributeName);
				result.put(name, value);
			}
		}
		return result;
	}

	/**
	 * Starts or restarts monitoring, the emitting of metrics records.
	 */
	public void startMonitoring() throws IOException {
		if (!isMonitoring)
		{
			startTimer();
			isMonitoring = true;
		}
	}

	/**
	 * Stops monitoring.	This does not free buffered data.
	 * @see #close()
	 */
	public void stopMonitoring() {
		if (isMonitoring)
		{
			shutdown();
			isMonitoring = false;
		}
	}

	/**
	 * Returns true if monitoring is currently in progress.
	 */
	public boolean isMonitoring() {
		return isMonitoring;
	}

	/**
	 * Stops monitoring and frees buffered data, returning this
	 * object to its initial state.
	 */
	public void close()
	{
		stopMonitoring();
		clearUpdaters();
	}

	/**
	 * Creates a new AbstractMetricsRecord instance with the given <code>recordName</code>.
	 * Throws an exception if the metrics implementation is configured with a fixed
	 * set of record names and <code>recordName</code> is not in that set.
	 *
	 * @param recordName the name of the record
	 * @throws AnalyticsException if recordName conflicts with configuration data
	 */
	public final void createRecord(String recordName)
	{
		if (bufferedData_.get(recordName) == null)
		{
			bufferedData_.put(recordName, new RecordMap());
		}
        recordMap_.put(recordName, new MetricsRecord(recordName, this));
	}

	/**
	 * Return the MetricsRecord associated with this record name.
	 * @param recordName the name of the record
	 * @return newly created instance of MetricsRecordImpl or subclass
	 */
	public MetricsRecord getMetricsRecord(String recordName)
	{
		return recordMap_.get(recordName);
	}

	/**
	 * Registers a callback to be called at time intervals determined by
	 * the configuration.
	 *
	 * @param updater object to be run periodically; it should update
	 * some metrics records
	 */
	public void registerUpdater(final IAnalyticsSource updater)
	{
		if (!updaters.contains(updater)) {
			updaters.add(updater);
		}
	}

	/**
	 * Removes a callback, if it exists.
	 *
	 * @param updater object to be removed from the callback list
	 */
	public void unregisterUpdater(IAnalyticsSource updater)
	{
		updaters.remove(updater);
	}

	private void clearUpdaters()
	{
		updaters.clear();
	}

	/**
	 * Starts timer if it is not already started
	 */
	private void startTimer()
	{
		if (timer == null)
		{
			timer = new Timer("Timer thread for monitoring AnalyticsContext", true);
			TimerTask task = new TimerTask()
			{
				public void run()
				{
					try
					{
						timerEvent();
					}
					catch (IOException ioe)
					{
						ioe.printStackTrace();
					}
				}
			};
			long millis = period_ * 1000;
			timer.scheduleAtFixedRate(task, millis, millis);
		}
	}

	/**
	 * Stops timer if it is running
	 */
	public void shutdown()
	{
		if (timer != null)
		{
			timer.cancel();
			timer = null;
		}
	}

	/**
	 * Timer callback.
	 */
	private void timerEvent() throws IOException
	{
		if (isMonitoring)
		{
			Collection<IAnalyticsSource> myUpdaters;

			// we dont need to synchronize as there will not be any
			// addition or removal of listeners
			myUpdaters = new ArrayList<IAnalyticsSource>(updaters);

			// Run all the registered updates without holding a lock
			// on this context
			for (IAnalyticsSource updater : myUpdaters)
			{
				try
				{
					updater.doUpdates(this);
				}
				catch (Throwable throwable)
				{
					throwable.printStackTrace();
				}
			}
			emitRecords();
		}
	}

	/**
	 *	Emits the records.
	 */
	private void emitRecords() throws IOException
	{
		for (String recordName : bufferedData_.keySet())
		{
			RecordMap recordMap = bufferedData_.get(recordName);
			synchronized (recordMap)
			{
				for (TagMap tagMap : recordMap.keySet())
				{
					MetricMap metricMap = recordMap.get(tagMap);
					OutputRecord outRec = new OutputRecord(tagMap, metricMap);
					emitRecord(recordName, outRec);
				}
			}
		}
		flush();
	}

	/**
	 * Called each period after all records have been emitted, this method does nothing.
	 * Subclasses may override it in order to perform some kind of flush.
	 */
	protected void flush() throws IOException
	{
	}

	/**
	 * Called by MetricsRecordImpl.update().	Creates or updates a row in
	 * the internal table of metric data.
	 */
	protected void update(MetricsRecord record)
	{
		String recordName = record.getRecordName();
		TagMap tagTable = record.getTagTable();
		Map<String,MetricValue> metricUpdates = record.getMetricTable();

		RecordMap recordMap = getRecordMap(recordName);
		synchronized (recordMap)
		{
			MetricMap metricMap = recordMap.get(tagTable);
			if (metricMap == null)
			{
				metricMap = new MetricMap();
				TagMap tagMap = new TagMap(tagTable); // clone tags
				recordMap.put(tagMap, metricMap);
			}
			for (String metricName : metricUpdates.keySet())
			{
				MetricValue updateValue = metricUpdates.get(metricName);
				Number updateNumber = updateValue.getNumber();
				Number currentNumber = metricMap.get(metricName);
				if (currentNumber == null || updateValue.isAbsolute())
				{
					metricMap.put(metricName, updateNumber);
				}
				else
				{
					Number newNumber = sum(updateNumber, currentNumber);
					metricMap.put(metricName, newNumber);
				}
			}
		}
	}

	private RecordMap getRecordMap(String recordName)
	{
		return bufferedData_.get(recordName);
	}

	/**
	 * Adds two numbers, coercing the second to the type of the first.
	 *
	 */
	private Number sum(Number a, Number b)
	{
		if (a instanceof Integer)
		{
			return new Integer(a.intValue() + b.intValue());
		}
		else if (a instanceof Float)
		{
			return new Float(a.floatValue() + b.floatValue());
		}
		else if (a instanceof Short)
		{
			return new Short((short)(a.shortValue() + b.shortValue()));
		}
		else if (a instanceof Byte)
		{
			return new Byte((byte)(a.byteValue() + b.byteValue()));
		}
		else
		{
			// should never happen
			throw new AnalyticsException("Invalid number type");
		}
	}

	/**
	 * Called by MetricsRecordImpl.remove().	Removes any matching row in
	 * the internal table of metric data.	A row matches if it has the same
	 * tag names and tag values.
	 */
	protected void remove(MetricsRecord record)
	{
		String recordName = record.getRecordName();
		TagMap tagTable = record.getTagTable();

		RecordMap recordMap = getRecordMap(recordName);

		recordMap.remove(tagTable);
	}

	/**
	 * Returns the timer period.
	 */
	public int getPeriod()
	{
		return period_;
	}

	/**
	 * Sets the timer period
	 */
	protected void setPeriod(int period)
	{
		this.period_ = period;
	}

	/**
	 * Sets the default port to listen on
	 */
	public void setPort(int port)
	{
		port_ = port;
	}

	/**
	 * Parses a space and/or comma separated sequence of server specifications
	 * of the form <i>hostname</i> or <i>hostname:port</i>.	If
	 * the specs string is null, defaults to localhost:defaultPort.
	 *
	 * @return a list of InetSocketAddress objects.
	 */
	private static List<InetSocketAddress> parse(String specs, int defaultPort)
	{
		List<InetSocketAddress> result = new ArrayList<InetSocketAddress>(1);
		if (specs == null) {
			result.add(new InetSocketAddress("localhost", defaultPort));
		}
		else {
			String[] specStrings = specs.split("[ ,]+");
			for (String specString : specStrings) {
				int colon = specString.indexOf(':');
				if (colon < 0 || colon == specString.length() - 1)
				{
					result.add(new InetSocketAddress(specString, defaultPort));
				} else
				{
					String hostname = specString.substring(0, colon);
					int port = Integer.parseInt(specString.substring(colon+1));
					result.add(new InetSocketAddress(hostname, port));
				}
			}
		}
		return result;
	}

	/**
	 * Starts up the analytics context and registers the VM metrics.
	 */
	public void start()
	{
		// register the vm analytics object with the analytics context to update the data
		registerUpdater(new VMAnalyticsSource());


        init("analyticsContext", DatabaseDescriptor.getGangliaServers());

		try
		{
			startMonitoring();
		}
		catch(IOException e)
		{
			logger_.error(LogUtil.throwableToString(e));
		}
	}

	public void stop()
	{
		close();
	}

    /**
     * Factory method that gets an instance of the StorageService
     * class.
     */
    public static AnalyticsContext instance()
    {
        if ( instance_ == null )
        {
        	AnalyticsContext.createLock_.lock();
            try
            {
                if ( instance_ == null )
                {
                    instance_ = new AnalyticsContext();
                }
            }
            finally
            {
                createLock_.unlock();
            }
        }
        return instance_;
    }
}
