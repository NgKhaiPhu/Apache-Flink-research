package b;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.util.Collector;
import java.util.Properties;

public class StreamingJob {
	public static void main(String[] args) throws Exception {
	// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		KafkaSource<String> source = KafkaSource.<String>builder()
				.setBootstrapServers("localhost:9092")
				.setTopics("News")
				.setStartingOffsets(OffsetsInitializer.earliest())
				.setValueOnlyDeserializer(new SimpleStringSchema())
				.build();

		DataStream<String> stream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

		/*Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "localhost:9092");
		properties.setProperty("zookeeper.connect", "localhost:2181");
		properties.setProperty("group.id", "test");
		DataStream<String> stream = env.addSource(new KafkaSource<>("news", new SimpleStringSchema(), properties));
		DataStream<WordWithCount> windowCounts = stream.
				flatMap(new FlatMapFunction<String, WordWithCount>() {
					@Override
					public void flatMap(String value, Collector<WordWithCount> out) {
						for (String word : value.split("\\s")) {
						out.collect(new WordWithCount(word, 1L));
						}
					}
				})
				.keyBy("word")
				.timeWindow(Time.seconds(5))
				.reduce(new ReduceFunction<WordWithCount>() {
						@Override
						public WordWithCount reduce(WordWithCount a, WordWithCount b) {
							return new WordWithCount(a.word, a.count + b.count);
						}
				});*/
		// print the results with a single thread, rather than in parallel
		DataStream<Tuple2<String, Integer>> windowCounts =
				stream.flatMap(new Tokenizer())
						.name("tokenizer")
						.keyBy(value -> value.f0)
						.sum(1)
						.name("counter");

		windowCounts.print().setParallelism(1);

		// execute program
		env.execute("Flink WordCount Streaming");
	}
// Data type for words with count
	/*public static class WordWithCount {
		public String word;
		public long count;
		public WordWithCount() {}
		public WordWithCount(String word, long count) {
			this.word = word;
			this.count = count;
		}
		@Override
		public String toString() {
			return word + " : " + count;
		}
	}*/
	public static final class Tokenizer
			implements FlatMapFunction<String, Tuple2<String, Integer>> {

		@Override
		public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
			// normalize and split the line
			String[] tokens = value.toLowerCase().split("\\W+");

			// emit the pairs
			for (String token : tokens) {
				if (token.length() > 0) {
					out.collect(new Tuple2<>(token, 1));
				}
			}
		}
	}
}
