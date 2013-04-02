package org.calrissian.accumulorecipes.eventstore.support;

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.calrissian.accumulorecipes.commons.iterators.TimeLimitingFilter;
import org.calrissian.accumulorecipes.eventstore.domain.Event;
import org.calrissian.accumulorecipes.eventstore.iterator.EventIntersectingIterator;
import org.calrissian.accumulorecipes.eventstore.iterator.EventIterator;
import org.calrissian.accumulorecipes.eventstore.support.query.validators.AndSingleDepthOnlyValidator;
import org.calrissian.criteria.domain.AndNode;
import org.calrissian.criteria.domain.EqualsLeaf;
import org.calrissian.criteria.domain.Leaf;
import org.calrissian.mango.collect.CloseableIterator;
import org.calrissian.mango.types.TypeContext;

import java.util.Collections;
import java.util.Date;

import static org.calrissian.accumulorecipes.eventstore.support.Constants.*;

public class QueryNodeHelper {

    protected final Connector connector;
    protected final String shardTable;
    protected final Integer numThreads;
    protected final Shard shard;

    protected final TypeContext typeContext = TypeContext.getInstance();

    public QueryNodeHelper(Connector connector, String shardTable, int numThreads, Shard shard) {
        this.connector = connector;
        this.shardTable = shardTable;
        this.numThreads = numThreads;
        this.shard = shard;
    }

    public CloseableIterator<Event> queryAndNode(Date start, Date stop, AndNode query, Authorizations auths)
            throws Exception {

        BatchScanner scanner = connector.createBatchScanner(shardTable, auths, numThreads);

        String[] range = shard.getRange(start, stop);

        IteratorSetting setting = new IteratorSetting(15, "timeLimit", TimeLimitingFilter.class);
        TimeLimitingFilter.setCurrentTime(setting, stop.getTime());
        TimeLimitingFilter.setTTL(setting, stop.getTime() - start.getTime());
        scanner.addScanIterator(setting);

        if (query != null && query.children() != null && query.children().size() > 1) {

            query.accept(new AndSingleDepthOnlyValidator());

            AndNodeColumns andNodeColumns = new AndNodeColumns(query);

            IteratorSetting is = new IteratorSetting(16, "eventIntersectingIterator", EventIntersectingIterator.class);
            EventIntersectingIterator.setColumnFamilies(is, andNodeColumns.getColumns(), andNodeColumns.getNotFlags());
            scanner.addScanIterator(is);

        } else {
            throw new IllegalArgumentException("You must have 2 or more items to query.");
        }

        scanner.setRanges(Collections.singleton(new Range(range[0], range[1] + DELIM_END)));

        return new EventScannerIterator(scanner);
    }


    public CloseableIterator<Event> querySingleLeaf(Date start, Date stop, Leaf query, Authorizations auths) throws Exception {

        BatchScanner scanner = connector.createBatchScanner(shardTable, auths, numThreads);

        String[] range = shard.getRange(start,  stop);

        IteratorSetting setting = new IteratorSetting(20, "timeLimit", TimeLimitingFilter.class);
        TimeLimitingFilter.setCurrentTime(setting, stop.getTime());
        TimeLimitingFilter.setTTL(setting, stop.getTime() - start.getTime());
        scanner.addScanIterator(setting);

        if (query != null) {

            if (query instanceof EqualsLeaf) {
                EqualsLeaf equalsLeaf = (EqualsLeaf) query;

                IteratorSetting iteratorSetting = new IteratorSetting(16, "eventIterator", EventIterator.class);
                scanner.addScanIterator(iteratorSetting);
                scanner.fetchColumnFamily(new Text(SHARD_PREFIX_B + DELIM + equalsLeaf.getKey() +
                                          DELIM + typeContext.getAliasForType(equalsLeaf.getValue()) +
                                          DELIM + typeContext.normalize(equalsLeaf.getValue())));
            } else {
                throw new IllegalArgumentException("The query " + query + " was not supported");
            }

        } else {
            throw new RuntimeException("Need to have a query and/or leaves of the query");
        }

        scanner.setRanges(Collections.singleton(new Range(range[0], range[1] + DELIM_END)));

        return new EventScannerIterator(scanner);
    }
}
