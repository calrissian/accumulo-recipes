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

import static org.calrissian.accumulorecipes.commons.support.Constants.NULL_BYTE;
import static org.calrissian.accumulorecipes.commons.support.Constants.ONE_BYTE;

/**
 * This filter will make sure no events pass through if they have geobounds that are no
 * within the range of the given bounding box. This is just in case the quad tree range
 * generation and the precision of overlap allowed do not leak out events that really
 * aren't within the given bounding box.
 */
public class BoundingBoxFilter extends Filter {

    private Rectangle2D.Double box;

    public static void setBoundingBox(IteratorSetting config, Rectangle2D.Double box) {
        Map<String, String> props = new HashMap<String, String>();
        props.put("x", Double.toString(box.getX()));
        props.put("y", Double.toString(box.getY()));
        props.put("width", Double.toString(box.getWidth()));
        props.put("height", Double.toString(box.getHeight()));
        config.addOptions(props);
    }

    @Override
    public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env) throws IOException {
        super.init(source, options, env);

        if (options.containsKey("x") && options.containsKey("y") && options.containsKey("width") && options.containsKey("height")) {

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
            int lastIdx = colF.lastIndexOf(NULL_BYTE);
            String geoCoords = colF.substring(lastIdx, colF.length() - 1);

            lastIdx = geoCoords.lastIndexOf(ONE_BYTE);
            Double x = Double.parseDouble(geoCoords.substring(0, lastIdx));
            Double y = Double.parseDouble(geoCoords.substring(lastIdx, geoCoords.length() - 1));

            return box.contains(x, y);
        } catch (Exception e) {
            return true;
        }
    }
}
