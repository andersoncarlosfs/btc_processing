/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.btc.controller.bolts;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.StandardCharsets;
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
     */
    private String time;
    private Double euro;
    private Double block_reward; // Reward since 12/05/2020

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
        this.block_reward = 6.25;
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

            if (object.containsKey("rate")) {
                this.time = (String) object.get("timestamp");
                this.euro = (Double) object.get("rate");
            }else {
                String string = "https://chain.api.btc.com/v3/block/" + object.get("hash");
                URLConnection urlConnection = new URL(string).openConnection();
                urlConnection.addRequestProperty("User-Agent", "Mozilla");
                urlConnection.setReadTimeout(5000);
                urlConnection.setConnectTimeout(5000);
                InputStream inputStream = urlConnection.getInputStream();

                JSONParser jsonParser = new JSONParser();
                JSONObject extra = (JSONObject) jsonParser.parse(
                        new InputStreamReader(inputStream, StandardCharsets.UTF_8)
                );

                extra = (JSONObject) ((JSONObject) extra.getOrDefault("data", new JSONObject())).getOrDefault("extras", new JSONObject());

                object.put("timestamp", this.time);
                object.put("found_by", (String) extra.get("pool_name"));
                object.put("block_reward_btc", block_reward);
                object.put("block_reward_euro", block_reward * this.euro);

                this.collector.emit(input.getSourceComponent(), new Values(object));
            }
            
            collector.ack(input);
        } catch (Exception e) {
            collector.reportError(e);
            collector.fail(input);
        }
    }

}
