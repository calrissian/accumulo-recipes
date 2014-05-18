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
package org.calrissian.accumulorecipes.commons.support;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.Writable;

import java.io.*;

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
