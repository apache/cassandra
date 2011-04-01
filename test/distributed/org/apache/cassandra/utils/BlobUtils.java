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
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import com.google.common.io.Files;
import org.apache.commons.configuration.Configuration;

import org.apache.whirr.service.ClusterSpec;

import org.jclouds.blobstore.BlobStoreContext;
import org.jclouds.blobstore.BlobStoreContextFactory;
import org.jclouds.blobstore.InputStreamMap;
import org.jclouds.blobstore.domain.BlobMetadata;

import org.jclouds.s3.S3AsyncClient;
import org.jclouds.s3.S3Client;
import org.jclouds.s3.domain.AccessControlList;
import org.jclouds.s3.domain.CannedAccessPolicy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class BlobUtils
{
    private static final Logger LOG = LoggerFactory.getLogger(BlobUtils.class);

    public static final String BLOB_PROVIDER = "whirr.blobstore.provider";
    public static final String BLOB_CONTAINER = "whirr.blobstore.container";
    
    private static BlobStoreContext getContext(Configuration config, ClusterSpec spec)
    {
        return new BlobStoreContextFactory().createContext(getProvider(config), spec.getIdentity(), spec.getCredential());
    }

    private static String getProvider(Configuration config)
    {
        String provider = config.getString(BLOB_PROVIDER, null);
        if (provider == null)
            throw new RuntimeException("Please set " + BLOB_PROVIDER + " to a jclouds supported provider.");
        return provider;
    }

    private static String getContainer(Configuration config)
    {
        String container = config.getString(BLOB_CONTAINER, null);
        if (container == null)
            throw new RuntimeException("Please set " + BLOB_CONTAINER + " to an existing container for your chosen provider.");
        return container;
    }

    /**
     * Stores the given local file as a public blob, and returns metadata for the blob.
     */
    public static Pair<BlobMetadata,URI> storeBlob(Configuration config, ClusterSpec spec, String filename)
    {
        File file = new File(filename);
        String container = getContainer(config);
        String provider = getProvider(config);

        // blob name and checksum of the file
        String blobName = System.nanoTime() + "/" + file.getName();
        String blobNameChecksum = blobName + ".md5";

        BlobStoreContext context = getContext(config, spec);

        File checksumFile;

        try
        {
            checksumFile = File.createTempFile("dtchecksum", "md5");
            checksumFile.deleteOnExit();

            FileWriter checksumWriter = new FileWriter(checksumFile);

            String checksum = FBUtilities.bytesToHex(Files.getDigest(file, MessageDigest.getInstance("MD5")));

            checksumWriter.write(String.format("%s  %s", checksum, file.getName()));
            checksumWriter.close();
        }
        catch (IOException e)
        {
            throw new RuntimeException("Can't create a checksum of the file: " + filename);
        }
        catch (NoSuchAlgorithmException e)
        {
            throw new RuntimeException(e.getMessage());
        }

        try
        {
            InputStreamMap map = context.createInputStreamMap(container);

            map.putFile(blobName, file);
            map.putFile(blobNameChecksum, checksumFile);

            // TODO: magic! in order to expose the blob as public, we need to dive into provider specific APIs
            // the hope is that permissions are encapsulated in jclouds in the future
            if (provider.contains("s3"))
            {
                S3Client sss = context.<S3Client,S3AsyncClient>getProviderSpecificContext().getApi();
                String ownerId = sss.getObjectACL(container, blobName).getOwner().getId();

                sss.putObjectACL(container,
                                 blobName,
                                 AccessControlList.fromCannedAccessPolicy(CannedAccessPolicy.PUBLIC_READ, ownerId));

                sss.putObjectACL(container,
                                 blobNameChecksum,
                                 AccessControlList.fromCannedAccessPolicy(CannedAccessPolicy.PUBLIC_READ, ownerId));
            }
            else
            {
                LOG.warn(provider + " may not be properly supported for tarball transfer.");
            }

            // resolve the full URI of the blob (see http://code.google.com/p/jclouds/issues/detail?id=431)
            BlobMetadata blob = context.getBlobStore().blobMetadata(container, blobName);
            URI uri = context.getProviderSpecificContext().getEndpoint().resolve("/" + container + "/" + blob.getName());
            return new Pair<BlobMetadata, URI>(blob, uri);
        }
        finally
        {
            context.close();
        }
    }

    public static void deleteBlob(Configuration config, ClusterSpec spec, BlobMetadata blob)
    {
        String container = getContainer(config);
        BlobStoreContext context = getContext(config, spec);
        try
        {
            context.getBlobStore().removeBlob(container, blob.getName());
        }
        finally
        {
            context.close();
        }
    }
}
