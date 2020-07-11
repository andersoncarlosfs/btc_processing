/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.btc.controller.bolts.elasticsearch;

import java.io.IOException;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.http.HttpHost;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

/**
 *
 * @author ?
 */
public abstract class BaseIndexerElasticSearchBolt extends BaseRichBolt {

    /**
     * 
     */
    private static RestHighLevelClient CLIENT;

    static {
        try {
            BaseIndexerElasticSearchBolt.CLIENT = new RestHighLevelClient(
                    RestClient.builder(
                            new HttpHost("localhost", 9200, "http"),
                            new HttpHost("localhost", 9201, "http")
                    )
            );
        } catch (Exception e) {
            System.err.println(e);
        }
    }
    
    /**
     * 
     * @param tuple 
     */
    @Override
    public void execute(Tuple tuple) {
        try {
            BaseIndexerElasticSearchBolt.CLIENT.index(
                    new IndexRequest(this.getTable()).source(this.toMap(tuple)), 
                    RequestOptions.DEFAULT
            );
        } catch (IOException e) {
            System.err.println(e);
        }
    }  
    
    /**
     * 
     * @return 
     */
    public abstract String getTable();
    
    /**
     * 
     * @param tuple
     * @return 
     */
    public abstract Map<String, Object> toMap(Tuple tuple);

}
