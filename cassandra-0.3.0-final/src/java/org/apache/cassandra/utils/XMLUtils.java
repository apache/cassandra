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

package org.apache.cassandra.utils;

import java.util.*;
import javax.xml.parsers.*;
import javax.xml.transform.*;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import java.io.*;
import org.w3c.dom.*;
import org.xml.sax.*;

/**
 * Author : Avinash Lakshman ( alakshman@facebook.com) & Prashant Malik ( pmalik@facebook.com )
 */

public class XMLUtils
{
	private Document document_;
    private XPath xpath_;

    public XMLUtils(String xmlSrc) throws FileNotFoundException, ParserConfigurationException, SAXException, IOException
    {        
        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        DocumentBuilder db = dbf.newDocumentBuilder();
        document_ = db.parse(xmlSrc);
        
        XPathFactory xpathFactory = XPathFactory.newInstance();
        xpath_ = xpathFactory.newXPath();
    }

	public String getNodeValue(String xql) throws XPathExpressionException
	{        
        XPathExpression expr = xpath_.compile(xql);
        String value = expr.evaluate(document_);
        if ( value != null && value.equals("") )
            value = null;
        return value;	
    }
        
	public String[] getNodeValues(String xql) throws XPathExpressionException
	{
        XPathExpression expr = xpath_.compile(xql);        
        NodeList nl = (NodeList)expr.evaluate(document_, XPathConstants.NODESET);
        int size = nl.getLength();
        String[] values = new String[size];
        
        for ( int i = 0; i < size; ++i )
        {
            Node node = nl.item(i);
            node = node.getFirstChild();
            values[i] = node.getNodeValue();
        }
        return values;       		
	}

	public NodeList getRequestedNodeList(String xql) throws XPathExpressionException
	{
        XPathExpression expr = xpath_.compile(xql);
        NodeList nodeList = (NodeList)expr.evaluate(document_, XPathConstants.NODESET);		
		return nodeList;
	}

	public static String getAttributeValue(Node node, String attrName) throws TransformerException
	{        
		String value = null;
		node = node.getAttributes().getNamedItem(attrName);
		if ( node != null )
		{
		    value = node.getNodeValue();
		}
		return value;
	}

    public static void main(String[] args) throws Throwable
    {
        XMLUtils xmlUtils = new XMLUtils("C:\\Engagements\\Cassandra-Golden\\storage-conf.xml");
        String[] value = xmlUtils.getNodeValues("/Storage/Seeds/Seed");
        System.out.println(value);
    }
}
