/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.btc.controller.bolts;

import java.net.URL;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import org.apache.storm.shade.org.apache.commons.codec.Charsets;
import org.apache.storm.shade.org.apache.commons.io.IOUtils;
import org.apache.storm.shade.org.json.simple.JSONObject;
import org.apache.storm.shade.org.json.simple.parser.JSONParser;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

/**
 *
 * @author ?
 */
public class QuerierBolt extends BaseRichBolt {


    /**
     *
     */
    private static final JSONParser PARSER = new JSONParser();
    
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
            QuerierBolt.PARSER.reset();

            JSONObject object = (JSONObject) input.getValueByField("source");

            String string = "https://chain.api.btc.com/v3/block/" + object.get("hash");

            JSONObject extra = (JSONObject) QuerierBolt.PARSER.parse(
                    IOUtils.toString(new URL(string), Charsets.UTF_8)
            );

            extra = (JSONObject) ((JSONObject) extra.getOrDefault("data", new JSONObject())).getOrDefault("extras", new JSONObject());

            object.put(
                    "timestamp",
                    ZonedDateTime.ofInstant(
                            Instant.ofEpochSecond((Long) object.get("timestamp")), ZoneId.systemDefault()
                    ).format(DateTimeFormatter.ISO_OFFSET_DATE_TIME)
            );

            object.put("found_by", (String) extra.get("pool_name"));
            
            this.collector.emit(input.getSourceComponent(), new Values(object));
            
            collector.ack(input);
        } catch (Exception e) {
            collector.reportError(e);
            collector.fail(input);
        }
    }

}
