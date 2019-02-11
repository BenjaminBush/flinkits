package org.ben.flinkits;

// Flink Imports
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.util.Collector;

// DL4J and ND4J Imports
import org.deeplearning4j.nn.modelimport.keras.KerasModel;
import org.deeplearning4j.nn.modelimport.keras.KerasModelImport;
import org.deeplearning4j.nn.modelimport.keras.KerasSequentialModel;
import org.deeplearning4j.nn.modelimport.keras.utils.KerasModelBuilder;
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.factory.Nd4j;

// JGraphT imports
import org.jgrapht.*;
import org.jgrapht.alg.connectivity.*;
import org.jgrapht.alg.interfaces.ShortestPathAlgorithm.*;
import org.jgrapht.alg.interfaces.*;
import org.jgrapht.alg.shortestpath.*;
import org.jgrapht.graph.*;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;


@SuppressWarnings("serial")
public class TrafficLSTMJob {
    public static void main(String[] args) throws Exception {
        ParameterTool parameter = ParameterTool.fromArgs(args);
        final String config_file_path = parameter.get("config");

        ParameterTool params = ParameterTool.fromPropertiesFile(config_file_path);

        // Level of parallelism
        final int parallelism = params.getInt("parallelism", 1);

        // Neural Net Configuration
        final String h5path = params.get("h5Path");
        double scale_ = params.getDouble("scale_", 0.0014925373134328358);
        double min_ = params.getDouble("min_", -0.03880597014925373);
        MultiLayerNetwork lstm = KerasModelImport.importKerasSequentialModelAndWeights(h5path, false);

        // Graph Configuration
        String[] cities = {"athens", "east_rancho_dominguez", "el_segundo", "long_beach", "lynwood", "wilmington"};
        Graph<String, DefaultWeightedEdge> graph = new SimpleDirectedWeightedGraph<String, DefaultWeightedEdge>(DefaultWeightedEdge.class);

        for (String city: cities) {
            graph.addVertex(city);
        }

        // Specify the true distance between edges. Calculated using latitude + longitude and curvature of Earth

        DefaultWeightedEdge e1 = graph.addEdge("el_segundo", "athens");
        graph.setEdgeWeight(e1, 4.854279482875291);

        DefaultWeightedEdge e2 = graph.addEdge("el_segundo", "wilmington");
        graph.setEdgeWeight(e2, 7.381129003403495);

        DefaultWeightedEdge e3 = graph.addEdge("athens", "lynwood");
        graph.setEdgeWeight(e3, 5.7221579249150185);

        DefaultWeightedEdge e4 = graph.addEdge("wilmington", "long_beach");
        graph.setEdgeWeight(e4, 1.8963114279968185);

        DefaultWeightedEdge e5 = graph.addEdge("lynwood", "east_rancho_dominguez");
        graph.setEdgeWeight(e5, 0.47837885698276855);

        DefaultWeightedEdge e6 = graph.addEdge("east_rancho_dominguez", "long_beach");
        graph.setEdgeWeight(e6, 0.8230426501818744);


        // Hashmap definitions and initializations
        HashMap<String, Double> average_speeds = new HashMap<String, Double>();
        for (String city: cities) {
            average_speeds.put(city, 0.0);
        }

        HashMap<String, Double> pred_flows = new HashMap<String, Double>();
        for (String city: cities) {
            pred_flows.put(city, 0.0);
        }


        // Kafka Configuration
        final String bootstrap_servers = params.get("bootstrap.servers", "localhost:9092");
        final String zookeeper_connect = params.get("zookeeper.connect", "localhost:2181");
        final String consumer_topic = params.get("consumer_topic", "input");
        final String producer_topic = params.get("producer_topic", "output");

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", bootstrap_servers);
        properties.setProperty("zookeeper.connect", zookeeper_connect);

        // Verify that we've loaded the LSTM and print its summary
        System.out.println("LSTM summary: " + lstm.summary());



        // get the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // get input data by connecting to Kafka
        FlinkKafkaConsumer011<String> consumer = new FlinkKafkaConsumer011(consumer_topic, new SimpleStringSchema(), properties);
        FlinkKafkaProducer011<String> producer = new FlinkKafkaProducer011(bootstrap_servers, producer_topic, new SimpleStringSchema());
        DataStream<String> dataStream = env.addSource(consumer);
        ((DataStreamSource<String>) dataStream).setParallelism(parallelism);

        // Parse the input data, then toss it through the LSTM
        DataStream<FlowsWithTimestamp> flows = dataStream
                .flatMap(new FlatMapFunction<String, FlowsWithTimestamp>() {
                    @Override
                    public void flatMap(String value, Collector<FlowsWithTimestamp> out) {
                        String[] split = value.split(";");
                        String city = split[0];
                        String avg_speed_str = split[1];
                        double avg_speed = Double.parseDouble(avg_speed_str);
                        average_speeds.put(city, avg_speed);
                        int[] actual_flows = new int[12];
                        double timestamp = Double.parseDouble(split[split.length-1]);
                        for (int i = 0; i < 12; i++) {
                            actual_flows[i] = (int) Double.parseDouble(split[i+1]);
                        }
                        System.out.println("Received something : " + value.toString());
                        out.collect(new FlowsWithTimestamp(city, avg_speed, actual_flows, 0, timestamp));
                    }
                })
                .map(new MapFunction<FlowsWithTimestamp, FlowsWithTimestamp>() {
                    @Override
                    public FlowsWithTimestamp map(FlowsWithTimestamp flowsWithTimestamp) throws Exception {
                        double[] flows = new double[12];
                        double predicted_flow;
                        int[] shape = {12, 1};
                        INDArray ndarr;
                        INDArray down1;
                        INDArray down2;
                        INDArray up1;
                        INDArray up2;
                        INDArray output;
                        List<INDArray> feedforward;

                        // Copy over ints to temporary double array
                        for (int i = 0; i < flowsWithTimestamp.actual_flows.length; i++) {
                            flows[i] = flowsWithTimestamp.actual_flows[i];
                        }

                        System.out.println("Original is : " + Arrays.toString(flows));

                        // Create the INDArray and specify the shape
                        ndarr = Nd4j.create(flows, shape);

                        // Scale so we can predict
                        down1 = ndarr.mul(scale_);
                        down2 = down1.add(min_);

                        // Use feedforward (not output!) and specify that we are in testing mode
                        feedforward = lstm.feedForward(down2, false);
                        output = feedforward.get(0);

                        // Scale back up
                        up1 = output.sub(scale_);
                        up2 = up1.div(scale_);

                        // Get the right index
                        predicted_flow = up2.getDouble(0);

                        pred_flows.put(flowsWithTimestamp.city, predicted_flow);

                        // Create new return object, return
                        FlowsWithTimestamp ret = new FlowsWithTimestamp(flowsWithTimestamp.city, flowsWithTimestamp.avg_speed, flowsWithTimestamp.actual_flows, (int) predicted_flow, flowsWithTimestamp.timestamp);
                        return ret;
                    }
                });

        // Print the results and specify the level of parallelism
        flows.print().setParallelism(parallelism);

        flows.map(new MapFunction<FlowsWithTimestamp, String>() {
            @Override
            public String map(FlowsWithTimestamp flowsWithTimestamp) throws Exception {
                // Run AStar and then send it to Kafka


                // Update the heuristic
                AStarAdmissibleHeuristic<String> heuristic = new AStarAdmissibleHeuristic<String>() {
                    @Override
                    public double getCostEstimate(String sourceVertex, String targetVertex) {
                        System.out.println("Source vertex is " + sourceVertex);
                        System.out.println("Target vertex is " + targetVertex);
                        double weight;
                        try {
                            DefaultWeightedEdge edge = graph.getEdge(sourceVertex, targetVertex);
                            weight = graph.getEdgeWeight(edge);
                        } catch (NullPointerException ne) {
                            System.out.println("Caught a null pointer, shouldn't be here");
                            weight = 0.0;
                        }
                        double speed_limit = 70.0;
                        double avg_speed = average_speeds.get(sourceVertex);
                        double predicted_flow = pred_flows.get(sourceVertex);
                        return Math.min(0, Math.min(weight, (70-avg_speed)*predicted_flow));
                    }
                };

                // Run AStar
                AStarShortestPath<String, DefaultWeightedEdge> astar = new AStarShortestPath<String, DefaultWeightedEdge>(graph, heuristic);
                GraphPath sp = astar.getPath("el_segundo", "long_beach");


                // Send the data
                String fwt = flowsWithTimestamp.toString();
                String path = sp.toString();
                String ret = fwt + ";" + path;
                return ret;
            }
        }).addSink(producer);

        // Execute the driver
        env.execute("TrafficLSTMJob");
    }

    // ------------------------------------------------------------------------

    /**
     * Data type for flows with timestamp
     */
    public static class FlowsWithTimestamp {
        public String city;
        public double avg_speed;
        public int[] actual_flows;
        public int predicted_flow;
        public double timestamp;

        public FlowsWithTimestamp() {}

        public FlowsWithTimestamp(String city, double avg_speed, int[] actual_flows, int predicted_flow, double timestamp) {
            this.city = city;
            this.avg_speed = avg_speed;
            this.actual_flows = actual_flows;
            this.predicted_flow = predicted_flow;
            this.timestamp = timestamp;
        }

        @Override
        public String toString() {
            StringBuilder ret = new StringBuilder();
            ret.append(this.city + ";");
            ret.append(this.avg_speed + ";");
            for (int i = 0; i < this.actual_flows.length; i++) {
                ret.append(this.actual_flows[i]);
                ret.append(';');
            }
            ret.append(predicted_flow);
            ret.append(';');
            ret.append(timestamp);
            return ret.toString();
        }
    }
}
