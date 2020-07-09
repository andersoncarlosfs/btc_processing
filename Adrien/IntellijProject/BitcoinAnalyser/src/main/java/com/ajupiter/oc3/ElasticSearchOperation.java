package com.ajupiter.oc3;

import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

import java.io.IOException;

public class ElasticSearchOperation {
    private RestHighLevelClient client;

    public ElasticSearchOperation() throws Exception {
        try {
            client = new RestHighLevelClient(
                    RestClient.builder(
                            new HttpHost("localhost", 9200, "http"),
                            new HttpHost("localhost", 9201, "http")));
        } catch (Exception e) {
            throw e;
        }

    }

    public void insert(IndexRequest request) throws IOException {
        client.index(request, RequestOptions.DEFAULT);
    }
}
