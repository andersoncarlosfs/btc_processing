/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.btc.controller.bolts;

import java.util.Map;
import org.apache.storm.shade.org.apache.commons.lang.StringEscapeUtils;
import org.apache.storm.shade.org.json.simple.JSONObject;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.joda.time.Instant;

/**
 *
 * @author ?
 */
public class SplitterBolt extends BaseRichBolt {
    
    /**
     *
     */
    private OutputCollector collector;

    /**
     *
     * @param declarer
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream("blocks", new Fields("source"));
        declarer.declareStream("transactions", new Fields("source"));
    }

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
            JSONObject object = (JSONObject) input.getValueByField("source");

            if (object.containsKey("total_amount")) {
                this.collector.emit("transactions", new Values(object));
            } else {
                if (object.containsKey("found_by")) {
                    object.put("timestamp", new Instant((long) (object.get("timestamp")) * 1000).toDateTime().toString());
                    this.collector.emit("blocks", new Values(object));
                } else {
                    System.out.println(input);
                }
            }
            
            collector.ack(input);
        } catch (Exception e) {
            collector.reportError(e);
            collector.fail(input);
        }
    }

}
