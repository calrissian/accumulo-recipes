package org.calrissian.accumulorecipes.commons.support;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.Writable;

import java.io.*;

/**
 * Created by cjnolet on 5/10/14.
 */
public class WritableUtils2 {

  public static byte[] serialize(Writable writable) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    DataOutputStream dataOut = null;
    try {
      dataOut = new DataOutputStream(out);
      writable.write(dataOut);
      return out.toByteArray();
    }
    finally {
      IOUtils.closeQuietly(dataOut);
    }
  }

  public static <T extends Writable> T asWritable(byte[] bytes, Class<T> clazz)
          throws IOException {
    T result = null;
    DataInputStream dataIn = null;
    try {
      result = clazz.newInstance();
      ByteArrayInputStream in = new ByteArrayInputStream(bytes);
      dataIn = new DataInputStream(in);
      result.readFields(dataIn);
    } catch (InstantiationException e) {
      // should not happen
      assert false;
    } catch (IllegalAccessException e) {
      // should not happen
      assert false;
    } finally {
      IOUtils.closeQuietly(dataIn);
    }
    return result;
  }
}
