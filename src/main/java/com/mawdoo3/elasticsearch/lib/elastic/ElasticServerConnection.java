package com.mawdoo3.elasticsearch.lib.elastic;


import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;


public class ElasticServerConnection {

    private static ElasticServerConnection instance;

    private RestHighLevelClient client;

    private ElasticServerConnection(String host, int port, String schema) {
        client = new RestHighLevelClient(RestClient.builder(new HttpHost(host, port, schema)));
    }

    public static ElasticServerConnection getInstance(String host, int port, String schema) {
        if (instance == null) {
            instance = new ElasticServerConnection(host, port, schema);
        }
        return instance;
    }

    public RestHighLevelClient getClient() {
        return client;
    }
}