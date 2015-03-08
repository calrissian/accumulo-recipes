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
import org.calrissian.mango.domain.entity.EntityBuilder;
import org.calrissian.mango.domain.entity.EntityIdentifier;
import org.calrissian.mango.domain.entity.EntityIdentifier;

public class EdgeEntity extends BaseEntity {

    public static final String HEAD = "head";
    public static final String TAIL = "tail";
    public static final String LABEL = "edgeLabel";

    @Deprecated
    public EdgeEntity(String type, String id, Entity head, String headVis, Entity tail, String tailVis, String label) {
        super(type, id);

        checkNotNull(head);
        checkNotNull(headVis);
        checkNotNull(tail);
        checkNotNull(tailVis);
        checkNotNull(label);


        Attribute headAttribute = new Attribute(HEAD, new EntityIdentifier(head), setVisibility(new HashMap<String, String>(1), headVis));
        Attribute tailAttribute = new Attribute(TAIL, new EntityIdentifier(tail), setVisibility(new HashMap<String, String>(1), tailVis));

        put(headAttribute);
        put(tailAttribute);
        put(new Attribute(LABEL, label));
    }

    @Deprecated
    public EdgeEntity(String type, String id, Entity head, Entity tail, String label) {
        this(type, id, head, "", tail, "", label);
    }

    public EdgeEntity(Entity entity) {
        super(entity);
    }

    public EntityIdentifier getHead() {
        if (this.get(HEAD) != null)
            return this.<EntityIdentifier>get(HEAD).getValue();
        return null;
    }

    public EntityIdentifier getTail() {
        if (this.get(TAIL) != null)
            return this.<EntityIdentifier>get(TAIL).getValue();
        return null;
    }

    public String getLabel() {
        if (this.get(LABEL) != null)
            return this.<String>get(LABEL).getValue();
        return null;
    }

    public static final class EdgeEntityBuilder extends EntityBuilder {

        public EdgeEntityBuilder(String type, String id, Entity head, String headVis, Entity tail, String tailVis, String label) {
            super(type, id);

            checkNotNull(head);
            checkNotNull(headVis);
            checkNotNull(tail);
            checkNotNull(tailVis);
            checkNotNull(label);

            Attribute headAttribute = new Attribute(HEAD, new EntityIdentifier(head), setVisibility(new HashMap<String, String>(1), headVis));
            Attribute tailAttribute = new Attribute(TAIL, new EntityIdentifier(tail), setVisibility(new HashMap<String, String>(1), tailVis));

            attr(headAttribute);
            attr(tailAttribute);
            attr(new Attribute(LABEL, label));
        }

        public EdgeEntityBuilder(String type, String id, Entity head, Entity tail, String label) {
            this(type, id, head, "", tail, "", label);
        }

        @Override
        public EdgeEntity build() {
            return new EdgeEntity(super.build());
        }

    }
}
