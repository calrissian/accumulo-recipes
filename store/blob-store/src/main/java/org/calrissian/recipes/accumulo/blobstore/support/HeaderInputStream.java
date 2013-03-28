package org.calrissian.recipes.accumulo.blobstore.support;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

public abstract class HeaderInputStream extends InputStream {

    protected Map<String,String> headers = new HashMap<String,String>();
    protected String name;

    public Map<String,String> getHeaders() {
        return headers;
    }

    public String getName() {
        return name;
    }

    protected void setHeaders(Map<String,String> headers) {
        this.headers = headers;
    }

    protected void setName(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return "HeaderInputStream{" +
                "name='" + name + '\'' +
                ", headers=" + headers +
                '}';
    }
}
