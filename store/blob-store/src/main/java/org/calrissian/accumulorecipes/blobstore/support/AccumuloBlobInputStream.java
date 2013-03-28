package org.calrissian.accumulorecipes.blobstore.support;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

public class AccumuloBlobInputStream extends HeaderInputStream {

    protected final Iterator<Map.Entry<Key,Value>> blobIterator;

    protected byte[] currentBlob;
    protected int curIndex = 0;

    public AccumuloBlobInputStream(Iterator<Map.Entry<Key, Value>> blobIterator) {
        this.blobIterator = blobIterator;

        boolean headersLoaded = false;
        while(!headersLoaded) {

            if(blobIterator.hasNext()) {

                Map.Entry<Key,Value> blobRow = blobIterator.next();

                if(getName() == null) {
                    setName(blobRow.getKey().getRow().toString());
                }

                if(blobRow.getKey().getColumnQualifier().toString().startsWith("\u0000")) {

                    String[] keyVal = blobRow.getKey().getColumnQualifier().toString().replaceFirst("\u0000", "")
                            .split("\u0000");

                    if(keyVal.length == 2) {

                        headers.put(keyVal[0], keyVal[1]);
                    }
                }

                else {
                    headersLoaded = true;
                    this.currentBlob = blobRow.getValue().get();
                }
            }
        }
    }

    @Override
    public int read() throws IOException {

        if(currentBlob == null) {
            return -1;
        }

        // if we've iterated through the current blob, load the next one
        if(curIndex == currentBlob.length) {

            if(!blobIterator.hasNext()) {
                return -1;
            }

            Map.Entry<Key,Value> nextEntry = blobIterator.next();

            currentBlob = nextEntry.getValue().get();
            curIndex = 0;
        }

        int curByte = (int)currentBlob[curIndex] & 0xff;

        curIndex += 1;

        return curByte;
    }
}
