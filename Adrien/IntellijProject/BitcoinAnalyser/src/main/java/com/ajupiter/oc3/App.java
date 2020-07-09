package com.ajupiter.oc3;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.topology.TopologyBuilder;


public class App {
    public static void main( String[] args ) throws Exception {

        TopologyBuilder builder = new TopologyBuilder();

        // Kafka Spout Configuration
        KafkaSpoutConfig.Builder<String, String> spoutConfigBuilder_rates = KafkaSpoutConfig.builder("localhost:9092", "rates");
        KafkaSpoutConfig.Builder<String, String> spoutConfigBuilder_transactions = KafkaSpoutConfig.builder("localhost:9092", "transactions");

        // Kafka Spout get "rates" and index into ES
        KafkaSpoutConfig<String, String> spoutConfig_rates = spoutConfigBuilder_rates.build();
        builder.setSpout("rates", new KafkaSpout<String, String>(spoutConfig_rates));
        builder.setBolt("rates-parsing", new BTCParsingBolt())
                .shuffleGrouping("rates");
        builder.setBolt("index-rates", new ESIndexRatesBolt())
                .shuffleGrouping("rates-parsing");

        // (TO DO) Kafka Spout get "transactions" and index into ES
        /*
        KafkaSpoutConfig<String, String> spoutConfig_transaction = spoutConfigBuilder_rates.build();
        builder.setSpout("transactions", new KafkaSpout<String, String>(spoutConfig_rates));
        builder.setBolt("transaction-parsing", new BTCParsingBolt())
                .shuffleGrouping("transactions");
        builder.setBolt("index-transactions", new ESIndexeRatesBolt())
                .shuffleGrouping("transactions-parsing");
        */

        StormTopology topology = builder.createTopology();

        Config config = new Config();
        config.setMessageTimeoutSecs(60*30);
        String topologyName = "btc";
        if(args.length > 0 && args[0].equals("remote")) {
            StormSubmitter.submitTopology(topologyName, config, topology);
        }
        else {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology(topologyName, config, topology);
        }
    }
}
