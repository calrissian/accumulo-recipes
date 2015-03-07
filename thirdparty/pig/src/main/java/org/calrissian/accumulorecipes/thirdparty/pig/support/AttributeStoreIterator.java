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
package org.calrissian.accumulorecipes.thirdparty.pig.support;

import java.util.Iterator;

import com.google.common.collect.AbstractIterator;
import org.calrissian.mango.domain.Attribute;
import org.calrissian.mango.domain.AttributeStore;

import static com.google.common.base.Preconditions.checkNotNull;

public class AttributeStoreIterator<T extends AttributeStore> extends AbstractIterator<Attribute> {

    Iterator<T> attributeCollections;
    T curAttributeCollection;
    Iterator<Attribute> attributes;

    public AttributeStoreIterator(Iterator<T> attributeCollections) {
        checkNotNull(attributeCollections);
        this.attributeCollections = attributeCollections;
    }

    @Override
    protected Attribute computeNext() {

        while((attributes == null || !attributes.hasNext()) &&
            attributeCollections.hasNext()) {
            curAttributeCollection = attributeCollections.next();
            attributes = curAttributeCollection.getAttributes().iterator();
        }

        if(attributes != null && attributes.hasNext())
            return attributes.next();

        return endOfData();
    }


    public T getTopStore() {
        return curAttributeCollection;
    }
}
