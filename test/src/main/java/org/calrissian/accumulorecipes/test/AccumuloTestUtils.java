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
package org.calrissian.accumulorecipes.test;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;

import java.util.Map;

public class AccumuloTestUtils {

    private AccumuloTestUtils() {
    }

    public static void dumpTable(Connector connector, String table, Authorizations auths) throws TableNotFoundException {
        Scanner scanner = connector.createScanner(table, auths);
        for (Map.Entry<Key, Value> entry : scanner) {
            System.out.println("ENTRY: " + entry);
        }
    }

    public static void dumpTable(Connector connector, String table) throws TableNotFoundException {
        dumpTable(connector, table, new Authorizations());
    }
}
