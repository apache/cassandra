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

package org.apache.cassandra.contrib.circuit;

import java.awt.BasicStroke;
import java.awt.Color;
import java.awt.Font;
import java.awt.Graphics;
import java.awt.Graphics2D;
import java.awt.Image;
import java.awt.RenderingHints;
import java.awt.geom.Ellipse2D;
import java.awt.geom.RoundRectangle2D;
import javax.swing.*;
import java.io.IOException;
import java.net.URL;
import java.util.Enumeration;
import java.util.List;

public class RingPanel extends JPanel
{
    private static final long serialVersionUID = 1L;
    private static final Color bgColor = Color.white;
    private static final String nodeImageFileOk = "images/node_green30x30.png";
    private static final String nodeImageFileSeed = "images/node_blue30x30.png";
    private static final String nodeImageFileShort = "images/node_red30x30.png";
    private static final String nodeImageFileUnknown = "images/node_yellow30x30.png";
    private static final Color ringColor = Color.blue;
    private static final Color fontColor = Color.black;
    private static final String fontName = "Helvetica";
    private static final int fontSize = 12;
    private static final int padding = 70;
    private static final int nodeDiameter = 30;
    private static final char[] verificationMessage = "Verifying ring...".toCharArray();
    
    private static RenderingHints defaultHints;
    private static Image nodeImageOk, nodeImageSeed, nodeImageShort, nodeImageUnknown;
    private RingModel ringModel;
    private boolean isVerifying = false;
    
    static
    {
        defaultHints = new RenderingHints(RenderingHints.KEY_ANTIALIASING,
                RenderingHints.VALUE_ANTIALIAS_ON);
        defaultHints.put(RenderingHints.KEY_RENDERING,
                RenderingHints.VALUE_RENDER_QUALITY);
    }
    
    public RingPanel(RingModel ringModel)
    {
        setBackground(bgColor);
        setFont(new Font(fontName, Font.PLAIN, fontSize));
        this.ringModel = ringModel;

        nodeImageOk = new ImageIcon(getClass().getClassLoader().getResource(nodeImageFileOk)).getImage();
        nodeImageSeed = new ImageIcon(getClass().getClassLoader().getResource(nodeImageFileSeed)).getImage();
        nodeImageShort = new ImageIcon(getClass().getClassLoader().getResource(nodeImageFileShort)).getImage();
        nodeImageUnknown = new ImageIcon(getClass().getClassLoader().getResource(nodeImageFileUnknown)).getImage();
    }

    public void paintComponent(Graphics g)
    {
        clear(g);
        Graphics2D g2d = (Graphics2D)g;
        g2d.setRenderingHints(defaultHints);

        // Draw the ring
        g2d.setPaint(ringColor);
        g2d.setStroke(new BasicStroke(1));
        int dia = getWidth() > getHeight() ? getHeight() - padding : getWidth() - padding;
        int radius = dia / 2;
        double ringX = (getWidth() / 2) - radius;
        double ringY = (getHeight() / 2) - radius;
        Ellipse2D.Double ring = new Ellipse2D.Double(ringX, ringY, dia, dia);
        g2d.draw(ring);

        // Obtain the ring according to our seed
        List<Node> nodes = ringModel.getNodes();
        
        // Place the nodes around the ring
        double current = 0;
        double increment = (2 * Math.PI) / nodes.size();
        
        // Draw each node
        for (Node node : nodes)
        {
            Image nodeImage = null;
            switch (node.nodeStatus)
            {
                case ISSEED:
                    nodeImage = nodeImageSeed;
                    break;
                case OK:
                    nodeImage = nodeImageOk;
                    break;
                case SHORT: case LONG:
                    nodeImage = nodeImageShort;
                    break;
                case UNKNOWN:
                    nodeImage = nodeImageUnknown;
                    break;
            }
            
            double x = Math.cos(current) * radius + (ring.getCenterX() - (nodeDiameter / 2));
            double y = Math.sin(current) * radius + (ring.getCenterY() - (nodeDiameter / 2));
            current = current + increment;
            
            g2d.drawImage(nodeImage, (int)x, (int)y, null);
            
            // Draw a square with rounded corners around the node to indicate
            // when it is "selected", or active.
            if (node.isSelected())
            {
                g2d.setPaint(ringColor);
                RoundRectangle2D.Double outline = new RoundRectangle2D.Double(
                        (x - 2), (y - 2), (nodeDiameter + 4), (nodeDiameter + 4), 10, 10);
                g2d.draw(outline);
            }
            
            // Write the label;
            g2d.setPaint(fontColor);
            g2d.drawString(node.getHost(), (int)x - (nodeDiameter / 2), (int)y - 5);
        }

        // if we're in the middle of verifying, draw something to indicate we are doing that.
        if (isVerifying)
        {
            int msgWidth = g2d.getFontMetrics().charsWidth(verificationMessage, 0, verificationMessage.length);
            g2d.setColor(new Color(128, 128, 128, 128));
            g2d.fillRect(0, 0, getWidth(), getHeight());
            g2d.setColor(fontColor);
            g2d.drawChars(verificationMessage, 0, verificationMessage.length, getWidth()/2 - msgWidth/2, getHeight()/2);
        }
    }

    public void setVerifying(boolean b)
    {
        if (b != isVerifying) {
            isVerifying = b;
            SwingUtilities.invokeLater(new Runnable() {public void run() {
                repaint();
            }});
        }
    }

    // super.paintComponent clears offscreen pixmap,
    // since we're using double buffering by default.
    protected void clear(Graphics g)
    {
        super.paintComponent(g);
    }
}
