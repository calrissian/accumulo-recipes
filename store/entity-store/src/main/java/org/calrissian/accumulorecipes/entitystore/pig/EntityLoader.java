package org.calrissian.accumulorecipes.entitystore.pig;

import com.google.common.collect.Iterators;
import com.google.common.collect.Multimap;
import groovy.lang.Binding;
import groovy.lang.GroovyShell;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.pig.LoadFunc;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.calrissian.accumulorecipes.commons.hadoop.RecordReaderValueIterator;
import org.calrissian.accumulorecipes.commons.transform.GettableTransform;
import org.calrissian.accumulorecipes.entitystore.hadoop.EntityInputFormat;
import org.calrissian.accumulorecipes.entitystore.model.EntityWritable;
import org.calrissian.mango.collect.TupleStoreIterator;
import org.calrissian.mango.criteria.builder.QueryBuilder;
import org.calrissian.mango.domain.entity.Entity;
import org.calrissian.mango.types.TypeRegistry;
import org.calrissian.mango.types.exception.TypeEncodingException;
import org.calrissian.mango.uri.support.UriUtils;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

import static com.google.common.collect.Sets.newHashSet;
import static java.util.Arrays.asList;
import static org.apache.commons.lang.StringUtils.splitPreserveAllTokens;
import static org.calrissian.mango.types.LexiTypeEncoders.LEXI_TYPES;
import static org.calrissian.mango.types.SimpleTypeEncoders.SIMPLE_TYPES;

public class EntityLoader extends LoadFunc {

    public static final String USAGE = "Usage: entity://indexTable/shardTable?user=&pass=&inst=&zk=&query=&types=&auths=[&fields=]";

    protected TupleStoreIterator<Entity> itr;
    protected TypeRegistry<String> registry = SIMPLE_TYPES;

    @Override
    public void setLocation(String uri, Job job) throws IOException {

        Configuration conf = job.getConfiguration();

        String path = uri.substring(uri.indexOf("://")+3, uri.indexOf("?"));

        String[] indexAndShardTable = StringUtils.splitPreserveAllTokens(path, "/");
        if(indexAndShardTable.length != 2)
            throw new IOException("Path portion of URI must contain the index and shard tables. " + USAGE);

        if(uri.startsWith("entity")) {
            String queryPortion = uri.substring(uri.indexOf("?")+1, uri.length());
            Multimap<String, String> queryParams = UriUtils.splitQuery(queryPortion);

            String accumuloUser = getProp(queryParams, "user");
            String accumuloPass = getProp(queryParams, "pass");
            String accumuloInst = getProp(queryParams, "inst");
            String zookeepers = getProp(queryParams, "zk");
            if(accumuloUser == null || accumuloPass == null || accumuloInst == null || zookeepers == null)
                throw new IOException("Some Accumulo connection information is missing. Must supply username, password, instance, and zookeepers. " + USAGE);

            String query = getProp(queryParams, "query");
            if(query == null)
                throw new IOException("Query builder groovy string must be supplied in the form of: q.and().eq('key1', 'val1').end(). ");

            String types = getProp(queryParams, "types");
            if(types == null)
                throw new IOException("A comma-separated list of entity types to load is required. " + USAGE);

            String auths = getProp(queryParams, "auths");
            if(auths == null)
                auths = "";     // default auths to empty

            String selectFields = getProp(queryParams, "fields");

            Set<String> fields = selectFields != null ? newHashSet(asList(splitPreserveAllTokens(selectFields, ","))) : null;
            Set<String> entitytypes = newHashSet(asList(splitPreserveAllTokens(types, ",")));

            QueryBuilder qb = null;
            try {
                // call groovy expressions from Java code
                Binding binding = new Binding();
                binding.setVariable("q", new QueryBuilder());
                GroovyShell shell = new GroovyShell(binding);
                qb = (QueryBuilder) shell.evaluate(query);
            } catch(Exception e) {
                throw new IOException("There was an error parsing the groovy query string. " + USAGE);
            }

            EntityInputFormat.setZooKeeperInstance(conf, accumuloInst, zookeepers);
            EntityInputFormat.setInputInfo(conf, accumuloUser, accumuloPass.getBytes(), new Authorizations(auths.getBytes()));
            try {
                EntityInputFormat.setQueryInfo(conf, entitytypes, qb.build(), fields, LEXI_TYPES);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        } else {
            throw new IOException("Location uri must begin with event://");
        }
    }

    private String getProp(Multimap<String, String> queryParams, String propKey) {
        Collection<String> props = queryParams.get(propKey);
        if (props.size() > 0)
            return props.iterator().next();
        return null;
    }

    @Override
    public InputFormat getInputFormat() throws IOException {
        return new EntityInputFormat();
    }

    @Override
    public void prepareToRead(RecordReader recordReader, PigSplit pigSplit) throws IOException {
        RecordReaderValueIterator<Key, EntityWritable> rri = new RecordReaderValueIterator<Key, EntityWritable>(recordReader);
        Iterator<Entity> xformed = Iterators.transform(rri, new GettableTransform<Entity>());
        itr = new TupleStoreIterator<Entity>(xformed);
    }


    @Override
    public Tuple getNext() throws IOException {

        if(!itr.hasNext())
            return null;

        org.calrissian.mango.domain.Tuple entityTuple = itr.next();

        /**
         * Create the pig tuple and hydrate with entity details. The format of the tuple is as follows:
         * (type, id, key, datatype, value, visiblity)
         */
        Tuple t = TupleFactory.getInstance().newTuple();
        t.append(itr.getTopStore().getType());
        t.append(itr.getTopStore().getId());
        t.append(entityTuple.getKey());
        try {
            t.append(registry.getAlias(entityTuple.getValue()));
            t.append(registry.encode(entityTuple.getValue()));
        } catch (TypeEncodingException e) {
            throw new RuntimeException(e);
        }
        t.append(entityTuple.getVisibility());

        return t;
    }
}
