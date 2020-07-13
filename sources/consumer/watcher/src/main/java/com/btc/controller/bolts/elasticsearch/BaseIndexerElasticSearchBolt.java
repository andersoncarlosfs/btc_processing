/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.btc.controller.bolts.elasticsearch;

import java.io.IOException;
import java.util.Map;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
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
     */
    private OutputCollector collector;

    /**
     *
     * @param topoConf
     * @param context
     * @param collector
     */
    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    /**
     *
     * @param tuple
     */
    @Override
    public void execute(Tuple input) {
        try {
            AbstractBaseElasticSearchBolt.CLIENT.index(
                    new IndexRequest(this.getTable()).source(this.toMap(input)),
                    RequestOptions.DEFAULT
            );
        } catch (IOException e) {
            System.err.println(e);
        } finally {
            this.collector.ack(input);
        }
    }

}
