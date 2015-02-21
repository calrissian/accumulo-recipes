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
package org.calrissian.accumulorecipes.commons.iterators.support;

import static org.apache.commons.lang.StringUtils.containsAny;
import static org.apache.commons.lang.StringUtils.replaceEach;
import static org.calrissian.accumulorecipes.commons.support.Constants.NULL_BYTE;
import static org.calrissian.accumulorecipes.commons.support.Constants.ONE_BYTE;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.calrissian.mango.criteria.domain.AbstractKeyValueLeaf;
import org.calrissian.mango.criteria.domain.AndNode;
import org.calrissian.mango.criteria.domain.EqualsLeaf;
import org.calrissian.mango.criteria.domain.GreaterThanEqualsLeaf;
import org.calrissian.mango.criteria.domain.GreaterThanLeaf;
import org.calrissian.mango.criteria.domain.HasLeaf;
import org.calrissian.mango.criteria.domain.HasNotLeaf;
import org.calrissian.mango.criteria.domain.InLeaf;
import org.calrissian.mango.criteria.domain.LessThanEqualsLeaf;
import org.calrissian.mango.criteria.domain.LessThanLeaf;
import org.calrissian.mango.criteria.domain.Node;
import org.calrissian.mango.criteria.domain.NotEqualsLeaf;
import org.calrissian.mango.criteria.domain.NotInLeaf;
import org.calrissian.mango.criteria.domain.OrNode;
import org.calrissian.mango.criteria.domain.ParentNode;
import org.calrissian.mango.criteria.domain.RangeLeaf;
import org.calrissian.mango.types.TypeRegistry;

public class NodeToJexl {

    /**
     * The following is a temporary fix to allow any characters to be used in keys as a workaround
     * for JEXL treating the chars as invalid (e.g. the key "key-1" is invalid because JEXL will treat
     * it as a term "key - 1". It only allows valid variable names to be keys, which is counter to
     * the what Accumulo allows- any arbitrary string).
     * @param key
     * @return
     */
    public static char[] badChars = "-!~#@~`%^&*()+=':;\"[]{}|\\/?<>,.".toCharArray();
    public static String[] strings;
    public static String[] chars;


    public static final String JEXL_NORM_PREFIX = "$__$";

    static {

        try {
            Map<Character,String> jexlNormalizationMap = new HashMap<Character,String>();
            for(int i = 0; i < badChars.length; i++)
                jexlNormalizationMap.put(badChars[i], i + "$");

            strings = new String[jexlNormalizationMap.size()];
            chars = new String[jexlNormalizationMap.size()];

            int count = 0;
            for(Map.Entry<Character, String> entry : jexlNormalizationMap.entrySet()) {
                strings[count] = JEXL_NORM_PREFIX + entry.getValue();
                count++;
            }

            count = 0;
            for(Map.Entry<Character, String> entry : jexlNormalizationMap.entrySet()) {
                chars[count] = entry.getKey().toString();
                count++;
            }
        } catch(Exception e) {
            e.printStackTrace();
        }
    }

    TypeRegistry<String> registry;

    /**
     * Normalizes invalid chars in a key if any invalid chars are found in the key
     * @param key
     * @return
     */
    public static String removeInvalidChars(String key) {
        if(containsAny(key, badChars))
            return replaceEach(key, chars, strings);

        return key;
    }

    /**
     * Reverts a string that had invalid chars normalized out back into the original key
     * @param fixedString
     * @return
     */
    public static String revertToOriginalkey(String fixedString) {
        if(fixedString.contains(JEXL_NORM_PREFIX))
            return replaceEach(fixedString, strings, chars);

        return fixedString;
    }

    public NodeToJexl(TypeRegistry<String> registry) {
        this.registry = registry;
    }


    public String transform(Set<String> types, Node node) {

        StringBuilder builder = new StringBuilder();
        Iterator<String> typesItr = types.iterator();
        while(typesItr.hasNext()) {
            builder.append(runTransform(typesItr.next(), node));
            if(typesItr.hasNext())
                builder.append(" or ");
        }

        return builder.toString();
    }

    private String runTransform(String type, Node node) {
        try {
            if (node instanceof ParentNode)
                return processParent(type, node);
            else
                return processChild(type, node);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private String processParent(String type, Node node) throws Exception {

        StringBuilder builder = new StringBuilder("(");
        for (int i = 0; i < node.children().size(); i++) {

            Node child = node.children().get(i);
            String newJexl;
            if (child instanceof ParentNode)
                newJexl = processParent(type, child);
            else
                newJexl = processChild(type, child);

            builder.append(newJexl);

            if (i < node.children().size() - 1) {
                if (node instanceof AndNode)
                    builder.append(" and ");
                else if (node instanceof OrNode)
                    builder.append(" or ");
            }
        }

        return builder.append(")").toString();

    }

    private String processChild(String type, Node node) throws Exception {

        StringBuilder builder = new StringBuilder();
        
        String leafKey = removeInvalidChars(((AbstractKeyValueLeaf)node).getKey());

        if (node instanceof EqualsLeaf) {
            builder.append("(");
            EqualsLeaf leaf = (EqualsLeaf) node;
            return builder.append(buildKey(type, leafKey)).append(" == '")
                .append(registry.getAlias(leaf.getValue())).append(ONE_BYTE)
                .append(registry.encode(leaf.getValue())).append("')")
                .toString();
        } else if (node instanceof NotEqualsLeaf) {
            builder.append("(");
            NotEqualsLeaf leaf = (NotEqualsLeaf) node;
            return builder.append(buildKey(type, leafKey)).append(" != '")
                .append(registry.getAlias(leaf.getValue())).append(ONE_BYTE)
                .append(registry.encode(leaf.getValue())).append("')")
                .toString();
        } else if (node instanceof RangeLeaf) {
            builder.append("(");
            RangeLeaf leaf = (RangeLeaf) node;
            return builder.append(buildKey(type, leafKey)).append(" >= '")
                .append(registry.getAlias(leaf.getStart())).append(ONE_BYTE)
                .append(registry.encode(leaf.getStart())).append("')")
                .append(" and (").append(buildKey(type, leafKey)).append(" <= '")
                .append(registry.getAlias(leaf.getEnd())).append(ONE_BYTE)
                .append(registry.encode(leaf.getEnd())).append("')")
                .toString();
        } else if (node instanceof GreaterThanLeaf) {
            builder.append("(");
            GreaterThanLeaf leaf = (GreaterThanLeaf) node;
            return builder.append(buildKey(type, leafKey)).append(" > '")
                .append(registry.getAlias(leaf.getValue())).append(ONE_BYTE)
                .append(registry.encode(leaf.getValue())).append("')")
                .toString();
        } else if (node instanceof GreaterThanEqualsLeaf) {
            builder.append("(");
            GreaterThanEqualsLeaf leaf = (GreaterThanEqualsLeaf) node;
            return builder.append(buildKey(type, leafKey)).append(" >= '")
                .append(registry.getAlias(leaf.getValue())).append(ONE_BYTE)
                .append(registry.encode(leaf.getValue())).append("')")
                .toString();
        } else if (node instanceof LessThanLeaf) {
            builder.append("(");
            LessThanLeaf leaf = (LessThanLeaf) node;
            return builder.append(buildKey(type, leafKey)).append(" < '")
                .append(registry.getAlias(leaf.getValue())).append(ONE_BYTE)
                .append(registry.encode(leaf.getValue())).append("')")
                .toString();
        } else if (node instanceof LessThanEqualsLeaf) {
            builder.append("(");
            LessThanEqualsLeaf leaf = (LessThanEqualsLeaf) node;
            return builder.append(buildKey(type, leafKey)).append(" <= '")
                .append(registry.getAlias(leaf.getValue())).append(ONE_BYTE)
                .append(registry.encode(leaf.getValue())).append("')")
                .toString();
        } else if (node instanceof HasLeaf) {
            builder.append("(");
            return builder.append(buildKey(type, leafKey)).append(" >= '").append(NULL_BYTE).append("')")
                .toString();
        } else if (node instanceof HasNotLeaf) {
            builder.append("!(");
            return builder.append(buildKey(type, leafKey)).append(" >= '").append(NULL_BYTE).append("')")
                .toString();
        } else if (node instanceof InLeaf) {
            builder.append("(");
            InLeaf leaf = (InLeaf) node;
            Collection<Object> objs = (Collection<Object>) leaf.getValue();
            Iterator<Object> objectIterator = objs.iterator();
            while(objectIterator.hasNext()) {
                Object val = objectIterator.next();
                builder.append(buildKey(type, leafKey)).append(" == '")
                    .append(registry.getAlias(val)).append(ONE_BYTE)
                    .append(registry.encode(val))
                    .toString();

                if(objectIterator.hasNext())
                    builder.append("' or ");
            }

            return builder.append("')").toString();

        } else if (node instanceof NotInLeaf) {
            builder.append("(");
            NotInLeaf leaf = (NotInLeaf) node;
            Collection<Object> objs = (Collection<Object>) leaf.getValue();
            Iterator<Object> objectIterator = objs.iterator();
            while(objectIterator.hasNext()) {
                Object val = objectIterator.next();
                builder.append(buildKey(type, leafKey)).append(" != '")
                    .append(registry.getAlias(val)).append(ONE_BYTE)
                    .append(registry.encode(val))
                    .toString();

                if(objectIterator.hasNext())
                    builder.append("' and ");
            }

            return builder.append("')").toString();

        } else {
            throw new RuntimeException("An unsupported leaf type was encountered: " + node.getClass().getName());
        }
    }

    protected String buildKey(String type, String key) {
        return key;
    }
}
