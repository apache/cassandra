package org.apache.cassandra.config;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.xpath.XPathExpressionException;

import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.XMLUtils;
import org.w3c.dom.NodeList;
import org.w3c.dom.Node;
import org.xml.sax.SAXException;
import org.yaml.snakeyaml.Dumper;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.introspector.Property;
import org.yaml.snakeyaml.nodes.NodeTuple;
import org.yaml.snakeyaml.nodes.Tag;
import org.yaml.snakeyaml.representer.Representer;

public class Converter {

    private static Config conf = new Config();
    private final static String PREVIOUS_CONF_FILE = "cassandra.xml";
    
    private static List<Keyspace> readTablesFromXml(XMLUtils xmlUtils) throws ConfigurationException
    {

        List<Keyspace> keyspaces = new ArrayList<Keyspace>();
        /* Read the table related stuff from config */
        try
        {
            NodeList tablesxml = xmlUtils.getRequestedNodeList("/Storage/Keyspaces/Keyspace");
            int size = tablesxml.getLength();
            for ( int i = 0; i < size; ++i )
            {
                String value = null;
                Keyspace ks = new Keyspace();
                Node table = tablesxml.item(i);
                /* parsing out the table ksName */
                ks.name = XMLUtils.getAttributeValue(table, "Name");

                
                ks.replica_placement_strategy = xmlUtils.getNodeValue("/Storage/Keyspaces/Keyspace[@Name='" + ks.name + "']/ReplicaPlacementStrategy");
                /* Data replication factor */
                value = xmlUtils.getNodeValue("/Storage/Keyspaces/Keyspace[@Name='" + ks.name + "']/ReplicationFactor");
                if (value != null)
                {
                    ks.replication_factor = Integer.parseInt(value);
                }
                
                    
                String xqlTable = "/Storage/Keyspaces/Keyspace[@Name='" + ks.name + "']/";
                NodeList columnFamilies = xmlUtils.getRequestedNodeList(xqlTable + "ColumnFamily");

                int size2 = columnFamilies.getLength();
                ks.column_families = new ColumnFamily[size2];
                for ( int j = 0; j < size2; ++j )
                {
                    Node columnFamily = columnFamilies.item(j);
                    ks.column_families[j] = new ColumnFamily();
                    ks.column_families[j].name = XMLUtils.getAttributeValue(columnFamily, "Name");
                    String xqlCF = xqlTable + "ColumnFamily[@Name='" + ks.column_families[j].name + "']/";
                    ks.column_families[j].column_type = XMLUtils.getAttributeValue(columnFamily, "ColumnType");
                    ks.column_families[j].compare_with = XMLUtils.getAttributeValue(columnFamily, "CompareWith");
                    
                    if (ks.column_families[j].column_type != null && ks.column_families[j].column_type.equals("Super"))
                        ks.column_families[j].compare_subcolumns_with = XMLUtils.getAttributeValue(columnFamily, "CompareSubcolumnsWith");
                    
                    if ((value = XMLUtils.getAttributeValue(columnFamily, "KeysCached")) != null)
                    {
                        ks.column_families[j].keys_cached = FBUtilities.parseDoubleOrPercent(value);
                    }
                    
                    if ((value = XMLUtils.getAttributeValue(columnFamily, "RowsCached")) != null)
                    {
                        ks.column_families[j].rows_cached = FBUtilities.parseDoubleOrPercent(value);
                    }
                    
                    if ((value = XMLUtils.getAttributeValue(columnFamily, "ReadRepairChance")) != null)
                    {
                        ks.column_families[j].read_repair_chance = FBUtilities.parseDoubleOrPercent(value);
                    }
                    
                    ks.column_families[j].comment = xmlUtils.getNodeValue(xqlCF + "Comment");
                }
                keyspaces.add(ks);
            }
            return keyspaces;
        }
        catch (XPathExpressionException e) 
        {
            throw new ConfigurationException("XPath expression error.");
        } 
        catch (TransformerException e) 
        {
            throw new ConfigurationException("Error occurred during the transformation process.");
        }
    }

    
    private static void loadPreviousConfig(String config) throws ConfigurationException
    {
        try {
            XMLUtils xmlUtils = new XMLUtils(config);
            conf.cluster_name = xmlUtils.getNodeValue("/Storage/ClusterName");
            
            String syncRaw = xmlUtils.getNodeValue("/Storage/CommitLogSync");
            conf.commitlog_sync = Config.CommitLogSync.valueOf(syncRaw);

            if (conf.commitlog_sync != null)
            {
                if (conf.commitlog_sync == Config.CommitLogSync.batch)
                    conf.commitlog_sync_batch_window_in_ms = Double.valueOf(xmlUtils.getNodeValue("/Storage/CommitLogSyncBatchWindowInMS"));
                else
                    conf.commitlog_sync_period_in_ms = Integer.valueOf(xmlUtils.getNodeValue("/Storage/CommitLogSyncPeriodInMS"));
                    
            }

            String modeRaw = xmlUtils.getNodeValue("/Storage/DiskAccessMode");
            conf.disk_access_mode = Config.DiskAccessMode.valueOf(modeRaw);
            
            conf.authenticator = xmlUtils.getNodeValue("/Storage/Authenticator");
            
            /* Hashing strategy */
            conf.partitioner = xmlUtils.getNodeValue("/Storage/Partitioner");
            
            conf.job_tracker_host = xmlUtils.getNodeValue("/Storage/JobTrackerHost");
            
            conf.job_jar_file_location = xmlUtils.getNodeValue("/Storage/JobJarFileLocation");
            
            String gcGrace = xmlUtils.getNodeValue("/Storage/GCGraceSeconds");
            if ( gcGrace != null )
                conf.gc_grace_seconds = Integer.parseInt(gcGrace);
            
            conf.initial_token = xmlUtils.getNodeValue("/Storage/InitialToken");
            
            String rpcTimeout = xmlUtils.getNodeValue("/Storage/RpcTimeoutInMillis");
            if ( rpcTimeout != null )
                conf.rpc_timeout_in_ms = Long.parseLong(rpcTimeout);
            
            String rawReaders = xmlUtils.getNodeValue("/Storage/ConcurrentReads");
            if (rawReaders != null)
            {
                conf.concurrent_reads = Integer.parseInt(rawReaders);
            }
            
            String rawWriters = xmlUtils.getNodeValue("/Storage/ConcurrentWrites");
            if (rawWriters != null)
            {
                conf.concurrent_writes = Integer.parseInt(rawWriters);
            }
            
            String rawFlushData = xmlUtils.getNodeValue("/Storage/FlushDataBufferSizeInMB");
            if (rawFlushData != null)
            {
                conf.flush_data_buffer_size_in_mb = Double.parseDouble(rawFlushData);
            }
            
            String rawFlushIndex = xmlUtils.getNodeValue("/Storage/FlushIndexBufferSizeInMB");
            if (rawFlushIndex != null)
            {
                conf.flush_index_buffer_size_in_mb = Double.parseDouble(rawFlushIndex);
            }

            String rawSlicedBuffer = xmlUtils.getNodeValue("/Storage/SlicedBufferSizeInKB");
            if (rawSlicedBuffer != null)
            {
                conf.sliced_buffer_size_in_kb = Integer.parseInt(rawSlicedBuffer);
            }

            String bmtThresh = xmlUtils.getNodeValue("/Storage/BinaryMemtableThroughputInMB");
            if (bmtThresh != null)
            {
                conf.binary_memtable_throughput_in_mb = Integer.parseInt(bmtThresh);
            }
            
            /* TCP port on which the storage system listens */
            String port = xmlUtils.getNodeValue("/Storage/StoragePort");
            if ( port != null )
                conf.storage_port = Integer.parseInt(port);
            
            /* Local IP or hostname to bind services to */
            conf.listen_address = xmlUtils.getNodeValue("/Storage/ListenAddress");
            
            conf.rpc_address = xmlUtils.getNodeValue("/Storage/RPCAddress");
            
            port = xmlUtils.getNodeValue("/Storage/RPCPort");
            if (port != null)
                conf.rpc_port = Integer.parseInt(port);
            
            String framedRaw = xmlUtils.getNodeValue("/Storage/ThriftFramedTransport");
            if (framedRaw != null)
            {
                conf.thrift_framed_transport = Boolean.valueOf(framedRaw);
            }
            
            conf.endpoint_snitch = xmlUtils.getNodeValue("/Storage/EndpointSnitch");
            
            String sbc = xmlUtils.getNodeValue("/Storage/SnapshotBeforeCompaction");
            if (sbc != null)
            {
                conf.snapshot_before_compaction = Boolean.valueOf(sbc);
            }
            
            String autoBootstr = xmlUtils.getNodeValue("/Storage/AutoBootstrap");
            if (autoBootstr != null)
            {
                conf.auto_bootstrap = Boolean.valueOf(autoBootstr);
            }
            
            String lifetime = xmlUtils.getNodeValue("/Storage/MemtableFlushAfterMinutes");
            if (lifetime != null)
                conf.memtable_flush_after_mins = Integer.parseInt(lifetime);
            
            String memtableSize = xmlUtils.getNodeValue("/Storage/MemtableThroughputInMB");
            if ( memtableSize != null )
                conf.memtable_throughput_in_mb = Integer.parseInt(memtableSize);
            
            String memtableObjectCount = xmlUtils.getNodeValue("/Storage/MemtableOperationsInMillions");
            if ( memtableObjectCount != null )
                conf.memtable_operations_in_millions = Double.parseDouble(memtableObjectCount);
            
            String columnIndexSize = xmlUtils.getNodeValue("/Storage/ColumnIndexSizeInKB");
            if(columnIndexSize != null)
            {
                conf.column_index_size_in_kb = Integer.parseInt(columnIndexSize);
            }

            String rowWarning = xmlUtils.getNodeValue("/Storage/RowWarningThresholdInMB");
            if (rowWarning != null)
            {
                conf.row_warning_threshold_in_mb = Long.parseLong(rowWarning);
            }
            
            conf.data_file_directories = xmlUtils.getNodeValues("/Storage/DataFileDirectories/DataFileDirectory");
            
            conf.commitlog_directory = xmlUtils.getNodeValue("/Storage/CommitLogDirectory");
            
            String value = xmlUtils.getNodeValue("/Storage/CommitLogRotationThresholdInMB");
            if ( value != null)
                conf.commitlog_rotation_threshold_in_mb = Integer.parseInt(value);
            
            conf.seeds = xmlUtils.getNodeValues("/Storage/Seeds/Seed");
            
            conf.keyspaces = readTablesFromXml(xmlUtils);
            
        } 
        catch (ParserConfigurationException e) {
            System.out.println("Parser error during previous config load.");
            throw new ConfigurationException("Parser error during previous config load.");
        } catch (SAXException e) {
            System.out.println("SAX error during previous config load.");
            throw new ConfigurationException("SAX error during previous config load.");
        } catch (IOException e) {
            System.out.println("File I/O error during previous config load.");
            throw new ConfigurationException("File I/O error during previous config load.");
        } catch (XPathExpressionException e) {
            System.out.println("XPath error during previous config load.");
            throw new ConfigurationException("XPath error during previous config load.");
        } 
    }
    
    private static void dumpConfig(String outfile) throws IOException
    {
        DumperOptions options = new DumperOptions();
        /* Use a block YAML arrangement */
        options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK);
        SkipNullRepresenter representer = new SkipNullRepresenter();
        /* Use Tag.MAP to avoid the class name being included as global tag */
        representer.addClassTag(Config.class, Tag.MAP);
        representer.addClassTag(ColumnFamily.class, Tag.MAP);
        Dumper dumper = new Dumper(representer, options);
        Yaml yaml = new Yaml(dumper);
        String output = yaml.dump(conf);
        
        /* Write to output file */
        BufferedWriter out = new BufferedWriter(new FileWriter(outfile));
        out.write("# Cassandra YAML generated from previous config\n");
        out.write("# Configuration wiki: http://wiki.apache.org/cassandra/StorageConfiguration\n");
        out.write(output);
        out.close(); 
    }
    
    public static void main (String[] args) 
    {
        try
        {
            String configname = null;
            ClassLoader loader = Converter.class.getClassLoader();
            URL scpurl = loader.getResource(PREVIOUS_CONF_FILE);
            if (scpurl == null)
                scpurl = loader.getResource("storage-conf.xml");
            
            if (scpurl != null)
                configname = scpurl.getFile();
            else 
                throw new ConfigurationException("Error finding previuos configuration file.");
            
            System.out.println("Found previous configuration: " + configname);
            
            loadPreviousConfig(configname);
            
            configname = configname.replace("cassandra.xml", "cassandra.yaml");
            
            System.out.println("Creating new configuration: " + configname);
            
            dumpConfig(configname);
        } 
        catch (IOException e)
        {
            System.out.println("Error creating new configuration file.");
            System.out.println(e.getMessage());
            e.printStackTrace();
        } 
        catch (ConfigurationException e) 
        {
            System.out.println("There was an error during config conversion.");
            System.out.println(e.getMessage());
            e.printStackTrace();
        }

    }
    
    /* used to prevent null values from being included in generated YAML */
    private static class SkipNullRepresenter extends Representer {
        protected NodeTuple representJavaBeanProperty(Object javaBean, Property property,
                Object propertyValue, Tag customTag) {
            if (propertyValue == null) {
                return null;
            } else {
                return super.representJavaBeanProperty(javaBean, property, propertyValue, customTag);
            }
        }

    }

}
