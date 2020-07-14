/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.btc.controller.bolts;

import java.util.Collections;
import java.util.Map;
import static java.util.Objects.requireNonNull;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.storm.elasticsearch.bolt.AbstractEsBolt;
import org.apache.storm.elasticsearch.common.DefaultEsTupleMapper;
import org.apache.storm.elasticsearch.common.EsConfig;
import org.apache.storm.elasticsearch.common.EsTupleMapper;
import org.apache.storm.shade.org.json.simple.JSONObject;
import org.apache.storm.shade.org.json.simple.parser.JSONParser;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;

/**
 *
 * @author ?
 */
public class ConverterBolt extends AbstractEsBolt {
    /**
     * 
     */
    private static final JSONParser PARSER = new JSONParser();

    
    /**
     * 
     */
    private final String index;
    
    /**
     * 
     */
    private final EsTupleMapper tupleMapper;
    
    /**
     * 
     */
    private String time;
    
    /**
     * 
     */
    private Double euro;   

    /**
     * EsIndexBolt constructor.     
     * @param index
     */
    public ConverterBolt(String index) {
        this(index, new EsConfig(), new DefaultEsTupleMapper());
    }
    
    
    /**
     * EsIndexBolt constructor.
     * @param index
     * @param esConfig Elasticsearch configuration containing node addresses {@link EsConfig}
     */
    public ConverterBolt(String index, EsConfig esConfig) {
        this(index, esConfig, new DefaultEsTupleMapper());
    }
    
    /**
     * EsIndexBolt constructor.
     * @param index
     * @param esConfig Elasticsearch configuration containing node addresses {@link EsConfig}
     * @param tupleMapper Tuple to ES document mapper {@link EsTupleMapper}
     */
    public ConverterBolt(String index, EsConfig esConfig, EsTupleMapper tupleMapper) {
        super(esConfig);
        
        this.index = index;
        
        this.tupleMapper = requireNonNull(tupleMapper);
    }

    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        super.prepare(map, topologyContext, outputCollector);
    }
    
    /**
     * {@inheritDoc}
     * Tuple should have relevant fields (source, index, type, id) for tupleMapper to extract ES document.
     */
    @Override
    public void process(Tuple tuple) {
        try {
            ConverterBolt.PARSER.reset();
            
            String string = StringEscapeUtils.unescapeJava(tuple.getString(4));
    
            JSONObject object = (JSONObject) ConverterBolt.PARSER.parse(string.substring(1, string.length() -1));
            
            if (tuple.getSourceComponent().equals("rates_kafka_spout")) {
                this.time = (String) object.get("timestamp");
                this.euro = (Double) object.get("rate");
            } else if (
                    this.euro != null && 
                    tuple.getSourceComponent().equals("transactions_kafka_spout") &&
                    object.containsKey("total_amount")
                    ) {
                
                object.put("timestamp", this.time);
                object.put("euro", ((Double) object.get("total_amount")) * this.euro);
                
                client.performRequest(
                    "post", 
                    "/" + index + "/_doc/", 
                    Collections.EMPTY_MAP, 
                    new StringEntity(object.toJSONString(), ContentType.APPLICATION_JSON)
                );
                    
            }
            
            collector.ack(tuple);
        } catch (Exception e) {
            collector.reportError(e);
            collector.fail(tuple);
        }
    }
    
}
