/*
 * Copyright (C) 2013 The Calrissian Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.calrissian.accumulorecipes.examples.blobstore;

import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.commons.io.IOUtils;
import org.calrissian.accumulorecipes.blobstore.impl.AccumuloBlobStore;
import org.calrissian.accumulorecipes.commons.domain.Auths;

import java.io.*;

/**
 * This example shows how to use the blob store to stream bytes from an input file into Accumulo rows and then further
 * stream the bytes from Accumulo back out to a file.
 */
public class FileStreamExample {

    private static final String SAMPLE_FILE_PATH = "/files/sampleFile.txt";

    public static void main(String args[]) throws AccumuloSecurityException, AccumuloException, TableExistsException, TableNotFoundException, IOException {

        if (args.length != 1) {
            System.out.println("Usage: " + FileStreamExample.class.getName() + " <outputDirectory>");
            System.exit(1);
        }

        String outputDir = args[0];

        // create our Accumulo connector
        Instance instance = new MockInstance();
        Connector connector = instance.getConnector("root", "secret".getBytes());

        // create a blob store with default chunk size of 1kb
        AccumuloBlobStore blobStore = new AccumuloBlobStore(connector, 1024);

        // let's load a file and stream it into the blob store
        File file = new File(FileStreamExample.class.getClassLoader().getResource("TestFile.txt").getFile());
        FileInputStream fis = new FileInputStream(file);
        OutputStream storageStream = blobStore.store(SAMPLE_FILE_PATH, "myFiles", file.lastModified(), "ABC");

        // copy and flush stream
        IOUtils.copy(fis, storageStream);
        storageStream.flush();
        storageStream.close();
        fis.close();

        System.out.println("File streamed from " + file.getAbsolutePath() + " into Accumulo path " + SAMPLE_FILE_PATH);

        // A new file to output our bytes from Accumulo
        file = new File(outputDir + "/sampleFile.txt");

        // Pull the bytes from Accumulo and stream to the new temporary file.
        FileOutputStream fos = new FileOutputStream(file);
        InputStream retrievalStream = blobStore.get(SAMPLE_FILE_PATH, "myFiles", new Auths("ABC"));

        // copy and flush stream
        IOUtils.copy(retrievalStream, fos);
        fos.flush();
        fos.close();
        retrievalStream.close();

        System.out.println("File streamed from " + SAMPLE_FILE_PATH + " and written to " + file.getAbsolutePath());
    }
}
