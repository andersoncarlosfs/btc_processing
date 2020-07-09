package com.ajupiter.oc3;

import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;

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

    public void insert(IndexRequest myrequest) throws IOException {
        client.index(myrequest, RequestOptions.DEFAULT);
    }
/*
    public static void main(String[] s){
        try{
            IndexRequest request = new IndexRequest("posts");
            request.id("1");
            String jsonString = "{" +
                    "\"user\":\"Hello\"," +
                    "\"postDate\":\"2013-01-30\"," +
                    "\"message\":\"trying out Elasticsearch\"" +
                    "}";
            request.source(jsonString, XContentType.JSON);
            ElasticSearchOperation elasticSearchOperation  = new ElasticSearchOperation();
            elasticSearchOperation.insert(request);
        }catch(Exception e) {
            e.printStackTrace();
        }
    }
*/
}
