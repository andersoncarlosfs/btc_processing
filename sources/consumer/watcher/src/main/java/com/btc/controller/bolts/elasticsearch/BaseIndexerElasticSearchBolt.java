/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.btc.controller.bolts.elasticsearch;

import java.io.IOException;
import java.util.Map;
import org.apache.storm.tuple.Tuple;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;

/**
 *
 * @author ?
 */
public abstract class BaseIndexerElasticSearchBolt extends AbstractBaseElasticSearchBolt {
    
    /**
     * 
     * @param tuple 
     */
    @Override
    public void execute(Tuple tuple) {
        try {
            AbstractBaseElasticSearchBolt.CLIENT.index(
                    new IndexRequest(this.getTable()).source(this.toMap(tuple)), 
                    RequestOptions.DEFAULT
            );
        } catch (IOException e) {
            System.err.println(e);
        }
    }  
    
}
