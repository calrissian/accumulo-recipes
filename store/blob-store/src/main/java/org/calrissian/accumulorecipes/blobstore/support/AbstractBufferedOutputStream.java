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
import java.io.OutputStream;
import java.util.Arrays;


/**
 * Utility class to allow for the management of writing a {@link java.io.OutputStream} data to a destination in chunks.
 *
 */
public abstract class AbstractBufferedOutputStream extends OutputStream {

    private byte[] buf;
    private int curr = 0;
    private boolean closed = false;

    public AbstractBufferedOutputStream(int bufferSize) {
        buf = new byte[bufferSize];
    }

    /**
     * Writes the buffer to the destination after it is either full, or has been flushed.
     * @param buf data to write
     * @throws java.io.IOException
     */
    protected abstract void writeBuffer(byte [] buf) throws IOException;

    /**
     * {@inheritDoc}
     */
    @Override
    public void write(int b) throws IOException {
        checkClosed();
        if (available() < 1)
            flush();

        buf[curr++] = (byte)b;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void flush() throws IOException {
        if (curr > 0) {
            writeBuffer(Arrays.copyOf(buf, curr));
            curr = 0;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() throws IOException {
        if (!closed) {
            closed = true;
            flush();
            super.close();
        }
    }

    /**
     * Provides the amount of buffer that is left to be written to.
     * @return number of bytes available in the buffer
     */
    protected int available() {
        return buf.length - curr;
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