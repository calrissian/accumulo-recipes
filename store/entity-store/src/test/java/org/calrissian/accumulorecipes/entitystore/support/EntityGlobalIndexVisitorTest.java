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
package org.calrissian.accumulorecipes.entitystore.support;

import static java.util.Arrays.asList;
import static org.calrissian.accumulorecipes.commons.support.Constants.DEFAULT_PARTITION_SIZE;
import static org.calrissian.accumulorecipes.entitystore.impl.AccumuloEntityStore.DEFAULT_IDX_TABLE_NAME;
import static org.calrissian.accumulorecipes.test.AccumuloTestUtils.dumpTable;
import static org.junit.Assert.assertEquals;
import java.util.Collections;
import java.util.Map;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.security.Authorizations;
import org.calrissian.accumulorecipes.commons.support.qfd.AttributeIndexKey;
import org.calrissian.accumulorecipes.commons.support.qfd.planner.visitors.GlobalIndexVisitor;
import org.calrissian.accumulorecipes.entitystore.EntityStore;
import org.calrissian.accumulorecipes.entitystore.impl.AccumuloEntityStore;
import org.calrissian.mango.criteria.builder.QueryBuilder;
import org.calrissian.mango.criteria.domain.Node;
import org.calrissian.mango.domain.Attribute;
import org.calrissian.mango.domain.entity.Entity;
import org.calrissian.mango.domain.entity.EntityBuilder;
import org.junit.Test;

public class EntityGlobalIndexVisitorTest {

    @Test
    public void test() throws AccumuloSecurityException, AccumuloException, TableExistsException, TableNotFoundException {

        Instance instance = new MockInstance();
        Connector connector = instance.getConnector("root", new PasswordToken("".getBytes()));
        EntityStore entityStore = new AccumuloEntityStore(connector);

        Entity entity = EntityBuilder.create("type", "id")
            .attr(new Attribute("key1", "val1"))
            .attr(new Attribute("key2", "val2"))
            .build();

        Entity entity2 = EntityBuilder.create("type", "id2")
            .attr(new Attribute("key1", "val1"))
            .attr(new Attribute("key2", "val2"))
            .attr(new Attribute("key3", true))
            .build();


        entityStore.save(asList(new Entity[]{entity, entity2}));

        dumpTable(connector, DEFAULT_IDX_TABLE_NAME);

        Node node = new QueryBuilder().and().eq("key1", "val1").eq("key2", "val2").eq("key3", true).end().build();

        BatchScanner scanner = connector.createBatchScanner(DEFAULT_IDX_TABLE_NAME, new Authorizations(), 2);
        GlobalIndexVisitor visitor = new EntityGlobalIndexVisitor(scanner,
                new EntityShardBuilder(DEFAULT_PARTITION_SIZE),
                Collections.singleton("type"));

        node.accept(visitor);
        visitor.exec();


        assertEquals(3, visitor.getCardinalities().size());
        for (Map.Entry<AttributeIndexKey, Long> entry : visitor.getCardinalities().entrySet()) {
            if (entry.getKey().getKey().equals("key1"))
                assertEquals(2l, (long) entry.getValue());
            else if (entry.getKey().getKey().equals("key2"))
                assertEquals(2l, (long) entry.getValue());
            else if (entry.getKey().getKey().equals("key3"))
                assertEquals(1l, (long) entry.getValue());
            else
                throw new RuntimeException("Unexpected key in cardinalities");
        }
    }
}
