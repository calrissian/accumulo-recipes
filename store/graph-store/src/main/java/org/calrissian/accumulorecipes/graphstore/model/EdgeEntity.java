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
package org.calrissian.accumulorecipes.graphstore.model;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.calrissian.accumulorecipes.commons.support.attribute.Metadata.Visiblity.setVisibility;
import java.util.HashMap;

import org.calrissian.mango.domain.Attribute;
import org.calrissian.mango.domain.entity.BaseEntity;
import org.calrissian.mango.domain.entity.Entity;
import org.calrissian.mango.domain.entity.EntityRelationship;

public class EdgeEntity extends BaseEntity {

    public static final String HEAD = "head";
    public static final String TAIL = "tail";
    public static final String LABEL = "edgeLabel";

    public EdgeEntity(String type, String id, Entity head, String headVis, Entity tail, String tailVis, String label) {
        super(type, id);

        checkNotNull(head);
        checkNotNull(headVis);
        checkNotNull(tail);
        checkNotNull(tailVis);
        checkNotNull(label);


        Attribute headAttribute = new Attribute(HEAD, new EntityRelationship(head), setVisibility(new HashMap<String, String>(1), headVis));
        Attribute tailAttribute = new Attribute(TAIL, new EntityRelationship(tail), setVisibility(new HashMap<String, String>(1), tailVis));

        put(headAttribute);
        put(tailAttribute);
        put(new Attribute(LABEL, label));
    }

    public EdgeEntity(String type, String id, Entity head, Entity tail, String label) {
        this(type, id, head, "", tail, "", label);
    }

    public EdgeEntity(Entity entity) {
        super(entity.getType(), entity.getId());
        putAll(entity.getAttributes());
    }

    public EntityRelationship getHead() {
        if (this.get(HEAD) != null)
            return this.<EntityRelationship>get(HEAD).getValue();
        return null;
    }

    public EntityRelationship getTail() {
        if (this.get(TAIL) != null)
            return this.<EntityRelationship>get(TAIL).getValue();
        return null;
    }

    public String getLabel() {
        if (this.get(LABEL) != null)
            return this.<String>get(LABEL).getValue();
        return null;
    }
}
