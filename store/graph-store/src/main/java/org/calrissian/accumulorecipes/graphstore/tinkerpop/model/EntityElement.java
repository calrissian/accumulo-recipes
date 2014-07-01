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
package org.calrissian.accumulorecipes.graphstore.tinkerpop.model;

import com.google.common.base.Preconditions;
import com.tinkerpop.blueprints.Element;
import org.calrissian.accumulorecipes.commons.domain.Auths;
import org.calrissian.accumulorecipes.graphstore.GraphStore;
import org.calrissian.mango.domain.Tuple;
import org.calrissian.mango.domain.entity.Entity;
import org.calrissian.mango.domain.entity.EntityIndex;

import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;

public class EntityElement implements Element {

    protected Entity entity;
    protected GraphStore graphStore;
    protected Auths auths;

    public EntityElement(Entity entity, GraphStore graphStore, Auths auths) {
        checkNotNull(entity);
        checkNotNull(graphStore);
        this.entity = entity;
        this.graphStore = graphStore;
        this.auths = auths;
    }

    public Entity getEntity() {
        return entity;
    }

    public GraphStore getGraphStore() {
        return graphStore;
    }

    @Override
    public <T> T getProperty(String s) {
        checkNotNull(s);
        if(s.equals("type"))
            return (T) entity.getType();

        if(entity.get(s) == null)
            return null;

        return entity.<T>get(s).getValue();
    }

    @Override
    public Set<String> getPropertyKeys() {
        return entity.keys();
    }

    @Override
    public void setProperty(String s, Object o) {
        checkNotNull(s);
        checkNotNull(o);
        entity.put(new Tuple(s, o));
    }

    @Override
    public <T> T removeProperty(String s) {
        Preconditions.checkNotNull(s);
        return (T) entity.remove(s).getValue();
    }

    @Override
    public void remove() {

        //TODO: Figure out what this method does
    }

    @Override
    public Object getId() {
        return new EntityIndex(entity.getType(), entity.getId());
    }
}
