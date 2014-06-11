/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.calrissian.accumulorecipes.commons.iterators.support;

import org.apache.accumulo.core.data.Key;

import static org.calrissian.accumulorecipes.commons.support.Constants.NULL_BYTE;

public class FieldIndexKeyParser extends KeyParser {

    @Override
    public void parse(Key key) {
        super.parse(key);

        String[] colFamParts = this.keyFields.get(COLUMN_FAMILY_FIELD).split(NULL_BYTE);
        this.keyFields.put(FIELDNAME_FIELD, colFamParts.length >= 2 ? colFamParts[1] : "");

        String[] colQualParts = this.keyFields.get(COLUMN_QUALIFIER_FIELD).split(NULL_BYTE);
        this.keyFields.put(SELECTOR_FIELD, colQualParts.length >= 1 ? colQualParts[0] : "");
        this.keyFields.put(DATATYPE_FIELD, "");
        this.keyFields.put(UID_FIELD, colQualParts.length >= 2 ? colQualParts[1] : "");
    }

    @Override
    public BaseKeyParser duplicate() {
        return new FieldIndexKeyParser();
    }

    @Override
    public String getSelector() {
        return keyFields.get(SELECTOR_FIELD);
    }

    @Override
    public String getDataType() {
        return keyFields.get(DATATYPE_FIELD);
    }

    @Override
    public String getFieldName() {
        return keyFields.get(FIELDNAME_FIELD);
    }

    @Override
    public String getUid() {
        return keyFields.get(UID_FIELD);
    }

    public String getDataTypeUid() {
        return getDataType() + NULL_BYTE + getUid();
    }

    // An alias for getSelector
    public String getFieldValue() {
        return getSelector();
    }
}
