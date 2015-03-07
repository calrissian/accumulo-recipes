/*
 * Copyright (C) 2015 The Calrissian Authors
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
package org.calrissian.accumulorecipes.commons.iterators;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Filter;
import org.calrissian.accumulorecipes.commons.support.Constants;

/**
 * A simple filter to skip over keys where the content encoded in the value is either missing, or the number
 * of encoded attributes is 0.
 */
public class EmptyEncodedRowFilter extends Filter {

    @Override
    public boolean accept(Key key, Value value) {

        if(key.getColumnFamily().toString().startsWith(Constants.PREFIX_E)) {
            try {
                ByteArrayInputStream bais = new ByteArrayInputStream(value.get());
                DataInputStream dis = new DataInputStream(bais);
                long size = dis.readInt();
                return value.get().length > 0 &&  size > 0;
            } catch (IOException e) {
                return false;
            }
        }

        return true;
    }
}
