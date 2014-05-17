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
package org.calrissian.accumulorecipes.geospatialstore;


import org.calrissian.accumulorecipes.commons.domain.Auths;
import org.calrissian.mango.collect.CloseableIterable;
import org.calrissian.mango.domain.Event;

import java.awt.geom.Point2D;
import java.awt.geom.Rectangle2D;

/**
 * A store that allows {@link Event} objects to be indexed at a given geo location and
 * further queried back given a bounding box.
 */
public interface GeoSpatialStore {

    /**
     * Index store entries at the given point.
     */
    void put(Iterable<Event> entry, Point2D.Double location);

    /**
     * Return all {@link Event} objects that lie within the given bounding box
     */
    CloseableIterable<Event> get(Rectangle2D.Double location, Auths auths);
}
