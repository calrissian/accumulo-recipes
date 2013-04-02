package org.calrissian.accumulorecipes.eventstore.support;

import org.apache.hadoop.io.Text;
import org.calrissian.criteria.domain.AndNode;
import org.calrissian.criteria.domain.EqualsLeaf;
import org.calrissian.criteria.domain.Node;
import org.calrissian.criteria.domain.NotEqualsLeaf;
import org.calrissian.mango.types.TypeContext;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.calrissian.accumulorecipes.eventstore.support.Constants.DELIM;
import static org.calrissian.accumulorecipes.eventstore.support.Constants.SHARD_PREFIX_B;


public class AndNodeColumns {

    protected final TypeContext typeContext = TypeContext.getInstance();

    protected  final Text[] columns;
    protected  final boolean[] notFlags;


    public AndNodeColumns(AndNode query) {

        Map<String, Object> fields = new HashMap<String, Object>();
        List<String> notFields = new ArrayList<String>();
        for (Node node : query.children()) {
            if (node instanceof NotEqualsLeaf) {
                NotEqualsLeaf notEqualsLeaf = (NotEqualsLeaf) node;
                notFields.add(notEqualsLeaf.getKey());
                fields.put(notEqualsLeaf.getKey(), notEqualsLeaf.getValue());
            } else if (node instanceof EqualsLeaf) {
                EqualsLeaf equalsLeaf = (EqualsLeaf) node;
                fields.put(equalsLeaf.getKey(), equalsLeaf.getValue());
            }
        }

        this.columns = new Text[fields.size()];
        this.notFlags = new boolean[fields.size()];

        int i = 0;
        for(Map.Entry<String, Object> entry : fields.entrySet()) {

            this.columns[i] = new Text(SHARD_PREFIX_B + DELIM + entry.getKey() + DELIM + typeContext.getAliasForType(entry.getValue())
                    + DELIM + entry.getValue());

            this.notFlags[i] = notFields.contains(entry.getKey()) ? true : false;
            i++;
        }
    }

    public Text[] getColumns() {
        return columns;
    }

    public boolean[] getNotFlags() {
        return notFlags;
    }
}
