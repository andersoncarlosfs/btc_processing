package com.ajupiter.oc3;

import org.apache.kafka.common.utils.Utils;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Map;

public class ESIndexRatesBolt extends BaseRichBolt {
        private ElasticSearchOperation elasticSearchOperation;
        private IndexRequest request;
        private String format_date;
        private SimpleDateFormat sdf;


    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        try {
            this.elasticSearchOperation = new ElasticSearchOperation();
        } catch (Exception e) {
            e.printStackTrace();
        }
        this.format_date = "MMM dd',' yyyy 'at' HH:mm z";
        this.sdf =  new SimpleDateFormat(format_date);
    }

    @Override
    public void execute(Tuple tuple) {
        String time_uk = tuple.getStringByField("time_uk");
        Double euro_rate = tuple.getDoubleByField("euro_rate");
        XContentBuilder builder;
        try {
            builder = XContentFactory.jsonBuilder();
            request = new IndexRequest("btc-rates");
            builder.startObject();
            {
                builder.timeField("time_uk", sdf.parse(time_uk));
                builder.field("euro_rate", euro_rate);
            }
            builder.endObject();
            request.source(builder);
            elasticSearchOperation.insert(request);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        }
        Utils.sleep(1000);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}