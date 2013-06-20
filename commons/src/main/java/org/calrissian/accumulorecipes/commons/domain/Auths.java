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
package org.calrissian.accumulorecipes.commons.domain;

import java.util.Collection;
import java.util.LinkedHashSet;

import static java.util.Arrays.asList;
import static org.apache.commons.lang.StringUtils.join;
import static org.apache.commons.lang.StringUtils.split;

/**
 * Wrapper class for Authorizations used by Accumulo
 */
public class Auths extends LinkedHashSet<String> {

    public static final String DELIM = ",";

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

    public String[] getAuths() {
        return toArray(new String[size()]);
    }

    public String serialize() {
        return toString();
    }

    @Override
    public String toString() {
        return join(this, DELIM);
    }
}
