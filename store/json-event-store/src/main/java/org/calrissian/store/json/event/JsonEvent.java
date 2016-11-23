/*
 * Copyright (C) 2015 The Calrissian Authors
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
package org.calrissian.store.json.event;

import java.util.Map;

public class JsonEvent {

    private String type;
    private String id;
    private long timestamp;

    private Map<String, Object> document;

    public JsonEvent(String type, String id, long timestamp, Map<String,Object> document) {
        this.type = type;
        this.id = id;
        this.timestamp = timestamp;
        this.document = document;
    }

    public String getType() {
        return type;
    }

    public String getId() {
        return id;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public Map<String,Object> getDocument() {
        return document;
    }
}
