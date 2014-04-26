package org.calrissian.accumulorecipes.geospatialstore;


import org.calrissian.accumulorecipes.commons.domain.Auths;
import org.calrissian.accumulorecipes.commons.domain.StoreEntry;
import org.calrissian.accumulorecipes.geospatialstore.model.BoundingBox;
import org.calrissian.mango.collect.CloseableIterable;

import java.awt.geom.Point2D;

public interface GeoSpatialStore {

    void put(StoreEntry entry, Point2D.Double location);

    CloseableIterable<StoreEntry> get(Point2D.Double location, Auths auths);

    CloseableIterable<StoreEntry> get(BoundingBox location, Auths auths);
}
