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
