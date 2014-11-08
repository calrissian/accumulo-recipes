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


import org.apache.accumulo.core.data.Value;

public class Constants {

    public static final int DEFAULT_PARTITION_SIZE = 7;

    public static final String PREFIX_E = "e";          // event prefix
    public static final String PREFIX_FI = "fi";        // field index index
    public static final String INDEX_V = "v";           // key index
    public static final String INDEX_K = "k";           // value index prefix

    public static final String NULL_BYTE = "\u0000";
    public static final String ONE_BYTE = "\u0001";
    public static final String TWO_BYTE = "\u0002";
    public static final String END_BYTE = "\uffff";

    public static final Value EMPTY_VALUE = new Value("".getBytes());

}
