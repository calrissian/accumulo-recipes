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

import java.util.Collections;
import java.util.Map;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchDeleter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;

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

    public static void clearTable(Connector connector, String table) throws AccumuloException, TableNotFoundException, AccumuloSecurityException {

        Authorizations userAuths = connector.securityOperations().getUserAuthorizations(connector.whoami());

        BatchDeleter batchDelete = connector.createBatchDeleter(table, userAuths, 1, new BatchWriterConfig());
        batchDelete.setRanges(Collections.singleton(new Range()));
        batchDelete.delete();
        batchDelete.close();

        batchDelete = connector.createBatchDeleter(table, userAuths, 1, new BatchWriterConfig());
        batchDelete.setRanges(Collections.singleton(new Range()));
        batchDelete.delete();
        batchDelete.close();
    }
}
