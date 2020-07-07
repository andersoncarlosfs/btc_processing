import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.topology.TopologyBuilder;

import java.net.UnknownHostException;

public class App {
    public static void main( String[] args ) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {


        TopologyBuilder builder = new TopologyBuilder();

        KafkaSpoutConfig.Builder<String, String> spoutConfigBuilder_rates = KafkaSpoutConfig.builder("192.168.1.55:9092", "rates");
        spoutConfigBuilder_rates.setGroupId("rates");
        KafkaSpoutConfig.Builder<String, String> spoutConfigBuilder_transactions = KafkaSpoutConfig.builder("192.168.1.55:9092", "transactions");
        spoutConfigBuilder_transactions.setGroupId("transactions");

        // Rates
        KafkaSpoutConfig<String, String> spoutConfig_rates = spoutConfigBuilder_rates.build();
        builder.setSpout("rates", new KafkaSpout<String, String>(spoutConfig_rates));
        builder.setBolt("btc-parsing", new BTCParsingBolt())
                .shuffleGrouping("rates");

/*
		builder.setSpout("json-spout", new StringSpout());
		builder.setBolt("es-bolt", new EsBolt("customer", conf))
				.shuffleGrouping("json-spout");

/*
		EsConfig esConfig = new EsConfig("http://localhost:9300");
		EsTupleMapper tupleMapper = new DefaultEsTupleMapper();
		EsIndexBolt indexBolt = new EsIndexBolt(esConfig, tupleMapper);

		builder.setBolt("es-indexing", indexBolt)
				.shuffleGrouping("btc-parsing");

/*
    	builder.setBolt("rate", new CityStatsBolt().withTumblingWindow(BaseWindowedBolt.Duration.of(1000*10)))
    		.fieldsGrouping("rates", new Fields("city"));
*/

/*
        builder.setBolt("save-results",  new SaveResultsBolt())
                .shuffleGrouping("parsing-stats");
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
