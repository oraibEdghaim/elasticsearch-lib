package com.mawdoo3.elasticsearch.lib.beans;

import com.mawdoo3.elasticsearch.lib.beans.enums.RangeRelation;

public class RangeCriteria {

    private Object gt;
    private Object gte;
    private Object lt;
    private Object lte;
    private String format ;
    private RangeRelation relation = RangeRelation.INTERSECTS;
    private String timeZone;
    private boolean includeLower = true;
    private boolean includeUpper = true;

    public Object getGt() {
        return gt;
    }

    public void setGt(Object gt) {
        this.gt = gt;
    }

    public Object getGte() {
        return gte;
    }

    public void setGte(Object gte) {
        this.gte = gte;
    }

    public Object getLt() {
        return lt;
    }

    public void setLt(Object lt) {
        this.lt = lt;
    }

    public Object getLte() {
        return lte;
    }

    public void setLte(Object lte) {
        this.lte = lte;
    }

    public String getFormat() {
        return format;
    }

    public void setFormat(String format) {
        if (format == null) {
            throw new IllegalArgumentException("format cannot be null");
        } else {
            this.format = format;
        }
    }

    public RangeRelation getRelation() {
        return relation;
    }

    public void setRelation(RangeRelation relation) {
        if (relation == null) {
            throw new IllegalArgumentException("relation cannot be null");
        }else {
            this.relation = relation;
        }
    }

    public String getTimeZone() {
        return timeZone;
    }

    public void setTimeZone(String timeZone) {
        if (format == null) {
            throw new IllegalArgumentException("timezone can't be null");
        } else {
            this.timeZone = timeZone;
        }
    }
    public boolean isIncludeLower() {
        return includeLower;
    }

    public void setIncludeLower(boolean includeLower) {
        this.includeLower = includeLower;
    }

    public boolean isIncludeUpper() {
        return includeUpper;
    }

    public void setIncludeUpper(boolean includeUpper) {
        this.includeUpper = includeUpper;
    }
}
