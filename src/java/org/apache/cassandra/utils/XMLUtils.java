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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.xpath.*;

import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

public class XMLUtils
{
	private Document document_;
    private XPath xpath_;

    public XMLUtils(String xmlSrc) throws FileNotFoundException, ParserConfigurationException, SAXException, IOException
    {        
        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        DocumentBuilder db = dbf.newDocumentBuilder();
        File xmlFile = new File(xmlSrc);
        document_ = db.parse(xmlFile);
        
        XPathFactory xpathFactory = XPathFactory.newInstance();
        xpath_ = xpathFactory.newXPath();
    }

	public String getNodeValue(String xql) throws XPathExpressionException
	{
        String value = xpath_.compile(xql).evaluate(document_);
        return "".equals(value) ? null : value;
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
}
