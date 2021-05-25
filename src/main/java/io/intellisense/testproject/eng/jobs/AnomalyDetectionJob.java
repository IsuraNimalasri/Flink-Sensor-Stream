package io.intellisense.testproject.eng.jobs;

import io.intellisense.testproject.eng.function.AnomalousScore;
import io.intellisense.testproject.eng.function.PurifyRowEvents;
import io.intellisense.testproject.eng.function.TagSensorReadings;
import io.intellisense.testproject.eng.model.DataPoint;
import lombok.extern.slf4j.Slf4j;
import io.intellisense.testproject.eng.datasource.CsvDatasource;
import io.intellisense.testproject.eng.sink.influxdb.InfluxDBSink;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.table.sources.CsvTableSource;
import org.apache.flink.types.Row;

import java.io.InputStream;
import java.time.Instant;

@Slf4j
public class AnomalyDetectionJob {

    public static void main(String[] args) throws Exception {

        // Pass configFile location as a program argument:
        // --configFile config.local.yaml
        final ParameterTool programArgs = ParameterTool.fromArgs(args);
        final String configFile = programArgs.getRequired("configFile");
        final InputStream resourceStream = AnomalyDetectionJob.class.getClassLoader().getResourceAsStream(configFile);
        final ParameterTool configProperties = ParameterTool.fromPropertiesFile(resourceStream);
        // Get Stream Metadata
        final InputStream sensorMetaDataStream = AnomalyDetectionJob.class.getClassLoader()
                .getResourceAsStream(configProperties.get("sensorMetaRegistry"));
        final ParameterTool metadataProps = ParameterTool.fromPropertiesFile(sensorMetaDataStream);

        // Stream execution environment
        // ...you can add here whatever you consider necessary for robustness
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(configProperties.getInt("flink.parallelism", 1));
        env.getConfig().setGlobalJobParameters(configProperties);
        env.getConfig().setGlobalJobParameters(metadataProps);

        // Simple CSV-table datasource
        final String dataset = programArgs.get("sensorData", "data/sensor-data.csv");

        final CsvTableSource csvDataSource = CsvDatasource.of(dataset).getCsvSource();
        final WatermarkStrategy<Row> watermarkStrategy = WatermarkStrategy.<Row>forMonotonousTimestamps()
                .withTimestampAssigner((event, timestamp) -> timestampExtract(event));
        // Source Stream
        final DataStream<Row> sourceStream = csvDataSource.getDataStream(env)
                .assignTimestampsAndWatermarks(watermarkStrategy)
                .name("datasource-operator");

        // Datastream ETL
        final DataStream<Row> filteredStream = sourceStream
                 .filter(new PurifyRowEvents())
                 .name("events-purify-operator");

        final DataStream<Tuple3<String, String, Double>> mappedStream = filteredStream
                 .flatMap(new TagSensorReadings())
                 .name("flatmap-operator");
        final KeyedStream<Tuple3<String, String, Double>, Object> senorWiesStream = mappedStream
                .keyBy(value -> value.f1);

        final WindowedStream<Tuple3<String, String, Double>, Object, GlobalWindow> widowedStream = senorWiesStream
                .countWindow(100,50);

        final DataStream<DataPoint> processedStream = widowedStream
                .process(new AnomalousScore())
                .name("processing-operator");

        processedStream.print();
        // Sink
        final SinkFunction<DataPoint> influxDBSink = new InfluxDBSink<>(configProperties);

        processedStream.addSink(influxDBSink).name("sink-operator");

        final JobExecutionResult jobResult = env.execute("Anomaly Detection Job");
        log.info(jobResult.toString());
    }

    private static long timestampExtract(Row event) {
        final String timestampField = (String) event.getField(0);
        return Instant.parse(timestampField).toEpochMilli();
    }
}
