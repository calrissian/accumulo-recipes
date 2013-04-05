/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.calrissian.accumulorecipes.metricsstore.domain;

public class MetricUnit {

    private long timestamp;
    private String group;
    private String type;
    private String name;

    private String visibility;

    private MetricType metricType;

    private Long metric;

    public MetricUnit(long timestamp, String group, String type, String name, String visibility, MetricType metricType, Long metric) {
        this.timestamp = timestamp;
        this.group = group;
        this.type = type;
        this.name = name;
        this.visibility = visibility;
        this.metricType = metricType;
        this.metric = metric;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public String getGroup() {
        return group;
    }

    public String getType() {
        return type;
    }

    public String getName() {
        return name;
    }

    public String getVisibility() {
        return visibility;
    }

    public MetricType getMetricType() {
        return metricType;
    }

    public Long getMetric() {
        return metric;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof MetricUnit)) return false;

        MetricUnit that = (MetricUnit) o;

        if (timestamp != that.timestamp) return false;
        if (group != null ? !group.equals(that.group) : that.group != null) return false;
        if (metric != null ? !metric.equals(that.metric) : that.metric != null) return false;
        if (metricType != that.metricType) return false;
        if (name != null ? !name.equals(that.name) : that.name != null) return false;
        if (type != null ? !type.equals(that.type) : that.type != null) return false;
        if (visibility != null ? !visibility.equals(that.visibility) : that.visibility != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = (int) (timestamp ^ (timestamp >>> 32));
        result = 31 * result + (group != null ? group.hashCode() : 0);
        result = 31 * result + (type != null ? type.hashCode() : 0);
        result = 31 * result + (name != null ? name.hashCode() : 0);
        result = 31 * result + (visibility != null ? visibility.hashCode() : 0);
        result = 31 * result + (metricType != null ? metricType.hashCode() : 0);
        result = 31 * result + (metric != null ? metric.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "MetricUnit{" +
                "metric=" + metric +
                ", metricType=" + metricType +
                ", visibility='" + visibility + '\'' +
                ", name='" + name + '\'' +
                ", type='" + type + '\'' +
                ", group='" + group + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}
