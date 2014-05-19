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
package org.calrissian.accumulorecipes.commons.support.criteria;

public class BaseCardinalityKey implements CardinalityKey {

    protected String key;
    protected String normalizedValue;
    protected String alias;

    protected BaseCardinalityKey() {
    }

    public BaseCardinalityKey(String key, String value, String alias) {
        this.key = key;
        this.normalizedValue = value;
        this.alias = alias;
    }

    public String getKey() {
        return key;
    }

    public String getNormalizedValue() {
        return normalizedValue;
    }

    public String getAlias() {
        return alias;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null) return false;

        BaseCardinalityKey that = (BaseCardinalityKey) o;

        if (alias != null ? !alias.equals(that.alias) : that.alias != null) return false;
        if (key != null ? !key.equals(that.key) : that.key != null) return false;
        if (normalizedValue != null ? !normalizedValue.equals(that.normalizedValue) : that.normalizedValue != null)
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = key != null ? key.hashCode() : 0;
        result = 31 * result + (normalizedValue != null ? normalizedValue.hashCode() : 0);
        result = 31 * result + (alias != null ? alias.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "BaseCardinalityKey{" +
                "key='" + key + '\'' +
                ", normalizedValue='" + normalizedValue + '\'' +
                ", alias='" + alias + '\'' +
                '}';
    }
}
