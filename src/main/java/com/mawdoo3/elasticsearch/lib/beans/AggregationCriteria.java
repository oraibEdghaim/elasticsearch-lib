package com.mawdoo3.elasticsearch.lib.beans;

import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.range.RangeAggregator;

import java.util.ArrayList;
import java.util.List;

public class AggregationCriteria {
    private String fieldName;
    private Object fieldValue;
    private String aggregationName = "aggregation";
    private String subAggregationName ="subAggregation";
    private Integer sizeOfHits = null;
    private DateHistogramInterval timeInterval = new DateHistogramInterval("1");
    private List<RangeAggregator.Range> ranges = new ArrayList<>();
    private double fixedInterval;

    public String getFieldName() {
        return fieldName;
    }

    public AggregationCriteria setFieldName(String fieldName) {
        this.fieldName = fieldName;
        return this;
    }

    public Object getFieldValue() {
        return fieldValue;
    }

    public AggregationCriteria setFieldValue(Object fieldValue) {
        this.fieldValue = fieldValue;
        return this;
    }
    public String getAggregationName() {
        return aggregationName;
    }

    public AggregationCriteria setAggregationName(String aggregationName) {
        this.aggregationName = aggregationName;
        return this;
    }

    public String getSubAggregationName() {
        return subAggregationName;
    }

    public AggregationCriteria setSubAggregationName(String subAggregationName) {
        this.subAggregationName = subAggregationName;
        return this;
    }

    public Integer getSizeOfHits() {
        return sizeOfHits;
    }

    public AggregationCriteria setSizeOfHits(Integer sizeOfHits) {
        if(sizeOfHits < 0){
            throw new IllegalArgumentException("No negative size of hits allowed.");
        }
        this.sizeOfHits = sizeOfHits;
        return this;
    }

    public DateHistogramInterval getTimeInterval() {
        return timeInterval;
    }

    public AggregationCriteria setTimeInterval(DateHistogramInterval timeInterval) {
        this.timeInterval = timeInterval;
        return this;
    }

    public double getFixedInterval() {
        return fixedInterval;
    }

    public AggregationCriteria setFIxedInterval(double interval) {
        this.fixedInterval = interval;
        return this;
    }

    public List<RangeAggregator.Range> getRanges() {
        return ranges;
    }

    public AggregationCriteria setRanges(List<RangeAggregator.Range> ranges) {
        this.ranges = ranges;
        return this;
    }
}
