package org.ben.flinkits;

// Flink Imports
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.util.Collector;

// DL4J and ND4J Imports
import org.deeplearning4j.nn.modelimport.keras.KerasModelImport;
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.factory.Nd4j;

import java.util.Properties;


@SuppressWarnings("serial")
public class TrafficLSTMJob {
    public static void main(String[] args) throws Exception {
        // Define constants at start of program
        MultiLayerNetwork lstm = KerasModelImport.importKerasSequentialModelAndWeights("/home/ben/git/research/TrafficPrediction/lstm_model.h5");
        double scale_ = 0.0014925373134328358;
        double min_ = -0.03880597014925373;

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("zookeeper.connect", "localhost:2181");

        // Verify that we've loaded the LSTM and print its summary
        System.out.println("Loaded lstm at " + lstm.summary());


        // get the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // get input data by connecting to Kafka
        FlinkKafkaConsumer011<String> consumer = new FlinkKafkaConsumer011("input", new SimpleStringSchema(), properties);
        FlinkKafkaProducer011<String> producer = new FlinkKafkaProducer011("localhost:9092", "output", new SimpleStringSchema());
        DataStream<String> dataStream = env.addSource(consumer);

        // Parse the input data, then toss it through the LSTM
        DataStream<FlowsWithTimestamp> flows = dataStream
                .flatMap(new FlatMapFunction<String, FlowsWithTimestamp>() {
                    @Override
                    public void flatMap(String value, Collector<FlowsWithTimestamp> out) {
                        String[] split = value.split(";");
                        int[] actual_flows = new int[12];
                        double timestamp = Double.parseDouble(split[split.length-1]);
                        for (int i = 0; i < 12; i++) {
                            actual_flows[i] = Integer.parseInt(split[i]);
                        }
                        out.collect(new FlowsWithTimestamp(actual_flows, 0, timestamp));
                    }
                })
                .map(new MapFunction<FlowsWithTimestamp, FlowsWithTimestamp>() {
                    @Override
                    public FlowsWithTimestamp map(FlowsWithTimestamp flowsWithTimestamp) throws Exception {
                        double[] flows = new double[12];
                        double predicted_flow;
                        int[] shape = {12, 1};
                        INDArray ndarr;
                        INDArray output;
                        INDArray prediction;


                        // Copy over ints to temporary double array
                        for (int i = 0; i < flowsWithTimestamp.actual_flows.length; i++) {
                            flows[i] = flowsWithTimestamp.actual_flows[i];
                        }

                        // Create the INDArray and specify the shape
                        ndarr = Nd4j.create(flows, shape);

                        // Multiply down so that we can predict
                        ndarr.mul(scale_);

                        // Make the prediction (is this step working?)
                        output = lstm.output(ndarr);

                        // Scale back up
                        prediction = output.div(scale_);

                        // Get the right index and cast to an int
                        predicted_flow = prediction.getDouble(11);

                        System.out.println("Timestamp is : " + flowsWithTimestamp.timestamp);

                        // Create new return object, return
                        FlowsWithTimestamp ret = new FlowsWithTimestamp(flowsWithTimestamp.actual_flows, (int) predicted_flow, flowsWithTimestamp.timestamp);
                        return ret;
                    }
                });

        // Print the results and specify the level of parallelism
        flows.print().setParallelism(1);

        flows.map(new MapFunction<FlowsWithTimestamp, String>() {
            @Override
            public String map(FlowsWithTimestamp flowsWithTimestamp) throws Exception {
                return flowsWithTimestamp.toString();
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
        public int[] actual_flows;
        public int predicted_flow;
        public double timestamp;

        public FlowsWithTimestamp() {}

        public FlowsWithTimestamp(int[] actual_flows, int predicted_flow, double timestamp) {
            this.actual_flows = actual_flows;
            this.predicted_flow = predicted_flow;
            this.timestamp = timestamp;
        }

        @Override
        public String toString() {
            StringBuilder ret = new StringBuilder();
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