package com.ajupiter.oc3;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.util.Map;

public class ESIndexRatesBolt extends BaseRichBolt {
        private ElasticSearchOperation elasticSearchOperation;
        private IndexRequest request;

    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        try {
            this.elasticSearchOperation = new ElasticSearchOperation();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void execute(Tuple tuple) {
        String time_uk = tuple.getStringByField("time_uk");
        Double euro_rate = tuple.getDoubleByField("euro_rate");
        request = new IndexRequest("btc-rates");
        request.id("1");
        String jsonString = "{" +
                "\"time_uk\":" + "\"" + time_uk + "\"" + "," +
                "\"euro_rate\":" + "\"" + euro_rate + "\"" +
                "}";
        request.source(jsonString, XContentType.JSON);
        try {
            elasticSearchOperation.insert(request);
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("done");
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}