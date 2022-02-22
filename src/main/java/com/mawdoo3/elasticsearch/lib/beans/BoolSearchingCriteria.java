package com.mawdoo3.elasticsearch.lib.beans;

import com.mawdoo3.elasticsearch.lib.beans.enums.BoolQueryClause;

import java.util.HashMap;
import java.util.Map;

public class BoolSearchingCriteria {

    private Map<BoolQueryClause, SearchingCriteria> boolClauseCriteria;

    public BoolSearchingCriteria(){
        this.boolClauseCriteria = new HashMap<>();
    }

    public BoolSearchingCriteria(HashMap<BoolQueryClause, SearchingCriteria> boolSearchingCriteria ){
        this.boolClauseCriteria = boolSearchingCriteria;
    }

    public BoolSearchingCriteria(BoolQueryClause boolClause,SearchingCriteria boolCriteria){
        this.boolClauseCriteria = new HashMap<>();
        this.boolClauseCriteria.put(boolClause,boolCriteria);
    }

    public BoolSearchingCriteria addBoolCriteria (BoolQueryClause boolClause,SearchingCriteria boolCriteria){
        if(this.boolClauseCriteria == null){
            this.boolClauseCriteria = new HashMap<>();
        }
        this.boolClauseCriteria.put(boolClause,boolCriteria);
        return this;
    }

    public Map<BoolQueryClause, SearchingCriteria> getBoolClauseCriteria() {
        return boolClauseCriteria;
    }
}
