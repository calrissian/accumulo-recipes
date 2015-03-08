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
package org.calrissian.accumulorecipes.commons.domain;

import static java.util.Arrays.asList;
import static org.apache.commons.lang.StringUtils.join;
import static org.apache.commons.lang.StringUtils.split;
import java.util.Collection;
import java.util.LinkedHashSet;

import org.apache.accumulo.core.security.Authorizations;

/**
 * Wrapper class for Authorizations used by Accumulo
 */
public class Auths extends LinkedHashSet<String> {

    public static final String DELIM = ",";

    public static final Auths EMPTY = new Auths();

    public Auths() {
    }

    public Auths(int initialCapacity) {
        super(initialCapacity);
    }

    public Auths(Collection<? extends String> c) {
        super(c);
    }

    public Auths(String auths) {
        this(split(auths, DELIM));
    }

    public Auths(String... auths) {
        this(asList(auths));
    }

    public Authorizations getAuths() {
        return new Authorizations(toArray(new String[size()]));
    }

    public String serialize() {
        return toString();
    }

    @Override
    public String toString() {
        return join(this, DELIM);
    }
}
