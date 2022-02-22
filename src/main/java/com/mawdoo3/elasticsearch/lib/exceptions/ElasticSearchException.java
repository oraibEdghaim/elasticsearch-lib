package com.mawdoo3.elasticsearch.lib.exceptions;

public class ElasticSearchException extends Exception{

    public ElasticSearchException(String msg){
        super(msg);
    }
    public ElasticSearchException(String msg, Throwable ex){
        super(msg,ex);
    }
}
