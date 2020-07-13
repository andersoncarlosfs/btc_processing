/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.btc.controller.bolts;

import java.util.UUID;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

/**
 *
 * @author ?
 */
public class TransformerBolt extends BaseBasicBolt {
    
    /**
     * 
     * @param declarer 
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("source", "index", "type", "id"));
    }

    /**
     * 
     * @param input
     * @param collector 
     */
    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        collector.emit(new Values(input.getString(4), "rates", "rate", UUID.randomUUID().toString()));
    }
    
}
