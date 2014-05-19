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
package org.calrissian.accumulorecipes.commons.support;

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.calrissian.mango.collect.CloseableIterable;
import org.calrissian.mango.collect.FluentCloseableIterable;

import java.io.IOException;
import java.util.Iterator;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.Map.Entry;

public class Scanners {

    private Scanners() {/* private constructor */}

    /**
     * Converts a {@link ScannerBase} into a {@link CloseableIterable}
     */
    public static CloseableIterable<Entry<Key, Value>> closeableIterable(final ScannerBase scanner) {
        checkNotNull(scanner);
        return new FluentCloseableIterable<Entry<Key, Value>>() {
            @Override
            protected void doClose() throws IOException {
                if (scanner instanceof BatchScanner)
                    ((BatchScanner) scanner).close();
            }

            @Override
            protected Iterator<Entry<Key, Value>> retrieveIterator() {
                return scanner.iterator();
            }
        };
    }

}
