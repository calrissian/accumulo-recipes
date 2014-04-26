package org.calrissian.accumulorecipes.geospatialstore.support;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Filter;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

import java.awt.geom.Rectangle2D;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.calrissian.accumulorecipes.geospatialstore.impl.AccumuloGeoSpatialStore.DELIM;
import static org.calrissian.accumulorecipes.geospatialstore.impl.AccumuloGeoSpatialStore.DELIM_ONE;

/**
 * This filter will make sure no events pass through if they have geobounds that are no
 * within the range of the given bounding box. This is just in case the quad tree range
 * generation and the precision of overlap allowed do not leak out events that really
 * aren't within the given bounding box.
 */
public class BoundingBoxFilter extends Filter {

    private Rectangle2D.Double box;

    @Override
    public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env) throws IOException {
        super.init(source, options, env);

        if(options.containsKey("x") && options.containsKey("y") && options.containsKey("width") && options.containsKey("height")) {

            Double x = Double.parseDouble(options.get("x"));
            Double y = Double.parseDouble(options.get("y"));
            Double width = Double.parseDouble(options.get("width"));
            Double height = Double.parseDouble(options.get("height"));

            box = new Rectangle2D.Double(x, y, width, height);
        }
    }

    @Override
    public boolean accept(Key key, Value value) {

        try {
            String colF = key.getColumnFamily().toString();
            int lastIdx = colF.lastIndexOf(DELIM);
            String geoCoords = colF.substring(lastIdx, colF.length()-1);

            lastIdx = geoCoords.lastIndexOf(DELIM_ONE);
            Double x = Double.parseDouble(geoCoords.substring(0, lastIdx));
            Double y = Double.parseDouble(geoCoords.substring(lastIdx, geoCoords.length()-1));

            return box.contains(x, y);
        } catch(Exception e) {
            return true;
        }
    }

    public static void setBoundingBox(IteratorSetting config, Rectangle2D.Double box) {
        Map<String,String> props = new HashMap<String, String>();
        props.put("x", Double.toString(box.getX()));
        props.put("y", Double.toString(box.getY()));
        props.put("width", Double.toString(box.getWidth()));
        props.put("height", Double.toString(box.getHeight()));
        config.addOptions(props);
    }
}
