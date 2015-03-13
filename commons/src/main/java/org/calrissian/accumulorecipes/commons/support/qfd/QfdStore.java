/*
 * Copyright (C) 2015 The Calrissian Authors
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
package org.calrissian.accumulorecipes.commons.support.qfd;

import java.util.Collection;
import java.util.Set;

import org.calrissian.accumulorecipes.commons.domain.Auths;
import org.calrissian.mango.collect.CloseableIterable;

public interface QfdStore<ET,EIT> {

    void save(Iterable<ET> items);

    void flush() throws Exception;

    void shutdown() throws Exception;

    CloseableIterable<ET> get(Collection<EIT> typesAndIds, Set<String> selectFields, Auths auths);

    CloseableIterable<ET> get(Collection<EIT> typesAndIds, Auths auths);
}
