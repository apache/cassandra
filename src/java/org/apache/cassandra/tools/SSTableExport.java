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

package org.apache.cassandra.tools;

import java.io.EOFException;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Collection;
import java.util.Iterator;

import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.io.IteratingRow;
import org.apache.cassandra.io.SSTableReader;
import org.apache.cassandra.io.SSTableScanner;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

/**
 * Export SSTables to JSON format.
 */
public class SSTableExport
{
    private static final String OUTFILE_OPTION = "f";
    private static Options options;
    private static CommandLine cmd;
    
    static
    {
        options = new Options();
        Option optOutfile = new Option(OUTFILE_OPTION, true, "output file");
        optOutfile.setRequired(false);
        options.addOption(optOutfile);
    }
    
    private static String quote(String val)
    {
        return String.format("\"%s\"", val);
    }
    
    private static String asKey(String val)
    {
        return String.format("%s: ", quote(val));
    }
    
    private static String serializeColumns(Collection<IColumn> cols, AbstractType comp)
    {
        StringBuilder json = new StringBuilder("{");
        
        Iterator<IColumn> iter = cols.iterator();
        while (iter.hasNext())
        {
            IColumn column = iter.next();
            json.append(asKey(comp.getString(column.name())));
            json.append(quote(FBUtilities.bytesToHex(column.value())));
            if (iter.hasNext())
                json.append(", ");
        }
        
        json.append(" }");
        
        return json.toString();
    }
    
    private static String serializeRow(IteratingRow row) throws IOException
    {
        ColumnFamily cf = row.getColumnFamily();
        AbstractType comparator = cf.getComparator();
        StringBuilder json = new StringBuilder(asKey(row.getKey().key));
        
        if (cf.isSuper())
        {
            json.append("{ ");

            Iterator<IColumn> iter = cf.getSortedColumns().iterator();
            while (iter.hasNext())
            {
                IColumn column = iter.next();
                json.append(asKey(comparator.getString(column.name())));
                json.append(serializeColumns(column.getSubColumns(), comparator));
                if (iter.hasNext())
                    json.append(", ");
            }
            
            json.append(" }");
        }
        else
        {
            json.append(serializeColumns(cf.getSortedColumns(), comparator));
        }
     
        return json.toString();
    }
    
    /**
     * Export an SSTable and write the resulting JSON to a PrintStream.
     * 
     * @param ssTableFile the SSTable to export
     * @param outs PrintStream to write the output to
     * @throws IOException on failure to read/write input/output
     */
    public static void export(String ssTableFile, PrintStream outs) throws IOException
    {
        SSTableReader reader = SSTableReader.open(ssTableFile);
        SSTableScanner scanner = reader.getScanner();
        
        outs.println("{");
        
        while(scanner.hasNext())
        {
            IteratingRow row = scanner.next();
            try
            {
                String jsonOut = serializeRow(row);
                outs.print("  " + jsonOut);
                if (scanner.hasNext())
                    outs.println(",");
                else
                    outs.println();
            }
            catch (IOException ioexcep)
            {
                System.err.println("WARNING: Corrupt row " + row.getKey().key + " (skipping).");
                continue;
            }
            catch (OutOfMemoryError oom)
            {
                System.err.println("ERROR: Out of memory deserializing row " + row.getKey().key);
                continue;
            }
        }
        
        outs.println("}");
        outs.flush();
    }
    
    /**
     * Export an SSTable and write the resulting JSON to a file.
     * 
     * @param ssTableFile SSTable to export
     * @param outFile file to write output to
     * @throws IOException on failure to read/write SSTable/output file
     */
    public static void export(String ssTableFile, String outFile) throws IOException
    {
        PrintStream outs = new PrintStream(outFile);
        export(ssTableFile, outs);
    }
    
    /**
     * Export an SSTable and write the resulting JSON to standard out.
     * 
     * @param ssTableFile SSTable to export
     * @throws IOException on failure to read/write SSTable/standard out
     */
    public static void export(String ssTableFile) throws IOException
    {
        export(ssTableFile, System.out);
    }

    /**
     * Given arguments specifying an SSTable, and optionally an output file,
     * export the contents of the SSTable to JSON.
     *  
     * @param args command lines arguments
     * @throws IOException on failure to open/read/write files or output streams
     */
    public static void main(String[] args) throws IOException
    {
        String usage = String.format("Usage: %s [-f outfile] <sstable>%n", SSTableExport.class.getName());
        
        CommandLineParser parser = new PosixParser();
        try
        {
            cmd = parser.parse(options, args);
        } catch (ParseException e1)
        {
            System.err.println(e1.getMessage());
            System.err.println(usage);
            System.exit(1);
        }
        
        String outFile = cmd.getOptionValue(OUTFILE_OPTION);
        
        if (cmd.getArgs().length != 1)
        {
            System.err.println("You must supply exactly one sstable");
            System.err.println(usage);
            System.exit(1);
        }
        
        if (outFile != null)
        {
            export(cmd.getArgs()[0], outFile);
        }
        else
        {
            export(cmd.getArgs()[0]);
        }
        System.exit(0);
    }
}