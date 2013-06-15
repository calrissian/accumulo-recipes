/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements. See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License. You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package org.calrissian.accumulorecipes.blobstore.support;

import java.io.IOException;
import java.io.InputStream;


/**
 * Utility class to allow for the source of the stream data to control the buffer size.  Useful for when the source of
 * of the stream data can be provided in chunks that can be reassembled into a contiguous {@link java.io.InputStream)
 */
public abstract class AbstractBufferedInputStream extends InputStream {

    private static final int EOF = -1;
    private byte [] buf = null;
    private int curr = 0;
    private boolean closed = false;

    /**
     * Returns true if there is no more source data.
     * @return true if at the end of the source data.
     */
    protected abstract boolean isEOF();

    /**
     * Retrieves the next set of data from the source.
     * @return A byte array containing the next set of data.
     * @throws java.io.IOException
     */
    protected abstract byte [] getNextBuffer() throws IOException;

    /**
     * {@inheritDoc}
     */
    @Override
    public int read() throws IOException {
        while (available() < 1) {
            if (isEOF())
                return EOF;
            else
                loadBuffer();
        }

        return buf[curr++] & 0xff;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int available() throws IOException {
        checkClosed();
        if (buf == null)
            return 0;

        return buf.length - curr;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() throws IOException {
        if (!closed) {
            closed = true;
            super.close();
        }
    }

    /**
     * Retrieves the next set of data and resets the current index.
     * @throws java.io.IOException
     */
    private void loadBuffer() throws IOException {
        buf = getNextBuffer();
        curr = 0;
    }

    /**
     * Check if the stream has been closed.  If it has, it will throw an {@link java.io.IOException}.
     * @throws java.io.IOException
     */
    private void checkClosed() throws IOException {
        if (closed)
            throw new IOException("Cannot read from stream anymore.  It has been closed");
    }
}
