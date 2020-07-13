/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.btc.controller.bolts.elasticsearch;

import java.util.Map;
import org.apache.http.HttpHost;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

/**
 *
 * @author ?
 */
public abstract class AbstractBaseElasticSearchBolt extends BaseRichBolt {

    /**
     *
     */
    protected static RestHighLevelClient CLIENT;

    /**
     *
     */
    private OutputCollector collector;

    static {
        try {
            AbstractBaseElasticSearchBolt.CLIENT = new RestHighLevelClient(
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
     * @return
     */
    public abstract String getTable();

    /**
     *
     * @param tuple
     * @return
     */
    public abstract Map<String, Object> toMap(Tuple tuple);

    /**
     *
     * @param tuple
     */
    public abstract void process(Tuple tuple);

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
     * @param input
     */
    @Override
    public void execute(Tuple input) {
        try {
            process(input);
        } catch (Exception e) {
            System.err.println(e);
        } finally {
            this.collector.ack(input);
        } 
    }

}
