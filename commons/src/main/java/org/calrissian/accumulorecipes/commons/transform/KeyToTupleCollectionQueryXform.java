package org.calrissian.accumulorecipes.commons.transform;

import com.esotericsoftware.kryo.Kryo;
import com.google.common.base.Function;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.commons.lang.StringUtils;
import org.calrissian.accumulorecipes.commons.domain.TupleCollection;
import org.calrissian.accumulorecipes.commons.iterators.support.EventFields;
import org.calrissian.mango.domain.Tuple;
import org.calrissian.mango.types.TypeRegistry;
import org.calrissian.mango.types.exception.TypeDecodingException;

import java.util.Map;
import java.util.Set;

import static java.nio.ByteBuffer.wrap;
import static org.apache.commons.lang.StringUtils.splitPreserveAllTokens;
import static org.calrissian.accumulorecipes.commons.support.Constants.INNER_DELIM;

public abstract class KeyToTupleCollectionQueryXform<V extends TupleCollection> implements Function<Map.Entry<Key, Value>, V> {

  private Set<String> selectFields;
  private Kryo kryo;
  private TypeRegistry<String> typeRegistry;

  public KeyToTupleCollectionQueryXform(Kryo kryo, TypeRegistry<String> typeRegistry, Set<String> selectFields) {
    this.selectFields = selectFields;
    this.kryo = kryo;
    this.typeRegistry = typeRegistry;
  }

  protected Set<String> getSelectFields() {
    return selectFields;
  }

  protected Kryo getKryo() {
    return kryo;
  }

  protected TypeRegistry<String> getTypeRegistry() {
    return typeRegistry;
  }

  @Override
  public V apply(Map.Entry<Key, Value> keyValueEntry) {
    EventFields eventFields = new EventFields();
    eventFields.readObjectData(kryo, wrap(keyValueEntry.getValue().get()));
    V entry = buildTupleCollectionFromKey(keyValueEntry.getKey());
    for (Map.Entry<String, EventFields.FieldValue> fieldValue : eventFields.entries()) {
      if(selectFields == null || selectFields.contains(fieldValue.getKey())) {
        String[] aliasVal = splitPreserveAllTokens(new String(fieldValue.getValue().getValue()), INNER_DELIM);
        try {
          Object javaVal = typeRegistry.decode(aliasVal[0], aliasVal[1]);
          String vis = fieldValue.getValue().getVisibility().getExpression().length > 0 ? fieldValue.getValue().getVisibility().toString() : "";
          entry.put(new Tuple(fieldValue.getKey(), javaVal, vis));
        } catch (TypeDecodingException e) {
          throw new RuntimeException(e);
        }
      }
    }
    return entry;
  }

  protected abstract V buildTupleCollectionFromKey(Key k);
}
