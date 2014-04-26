package org.calrissian.accumulorecipes.geospatialstore;


import org.calrissian.accumulorecipes.commons.domain.Auths;
import org.calrissian.accumulorecipes.commons.domain.StoreEntry;
import org.calrissian.mango.collect.CloseableIterable;

import java.awt.geom.Point2D;
import java.awt.geom.Rectangle2D;

public interface GeoSpatialStore {

    void put(Iterable<StoreEntry> entry, Point2D.Double location);

    CloseableIterable<StoreEntry> get(Point2D.Double location, Auths auths);

    CloseableIterable<StoreEntry> get(Rectangle2D.Double location, Auths auths);
}
