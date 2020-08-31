package com.bu;

import com.google.api.client.json.JsonParser;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableSchema;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.FileBasedSink;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.joda.time.Duration;

/**
 * Read From Kafka Topic and Write to BigQuery. Errors records are stored in Error table
 */
public class Kafka2BQ {
  public static final String SHARD_TEMPLATE = "W-P-SS-of-NN";

  public static void main(String[] args) throws Exception {
    PipelineOptionsFactory.register(Kafka2BigQueryOptions.class);

    Kafka2BigQueryOptions customOptions = PipelineOptionsFactory.fromArgs(args)
        .as(Kafka2BigQueryOptions.class);

    final Counter successCounter = Metrics.counter("stats", "success");
    final Counter failureCounter = Metrics.counter("stats", "failure");

    TableReference bqTable = getTableReference(customOptions.getProject(), customOptions.getDataset().get(), customOptions.getTableName().get());
    TableReference bqTableError = getTableReference(customOptions.getProject(), customOptions.getDataset().get(), customOptions.getTableName().get() + "Err");

    Pipeline pipeline = Pipeline.create(customOptions);
    PCollection<String> rawPDCCollection = pipeline.apply(KafkaIO.<String, String>read()
        .withBootstrapServers(customOptions.getKafakBootstrapServer().get())
        .withTopic(customOptions.getTopicName().get())
        .withKeyDeserializerAndCoder(StringDeserializer.class, NullableCoder.of(StringUtf8Coder.of()))
        .withValueDeserializerAndCoder(StringDeserializer.class, NullableCoder.of(StringUtf8Coder.of()))
        .withoutMetadata()).apply("Extract Data", Values.create());

    rawPDCCollection.apply(
        "Creating Window",
        Window.into(
            FixedWindows.of(Duration.millis(50000)))).apply("Write to Archive Bucket", TextIO.write().to(
            new WindowedFilenamePolicy(
                customOptions.getArchiveBucket(),
                customOptions.getArchiveFilenamePrefix(),
                ValueProvider.StaticValueProvider.of(SHARD_TEMPLATE),
                ValueProvider.StaticValueProvider.of(".txt")))
            .withWindowedWrites()
        .withTempDirectory(
            FileBasedSink.convertToFileResourceIfPossible(customOptions.getGcpTempLocation())
                .getCurrentDirectory())
        .withNumShards(1));


    PCollectionTuple processedCollection = CSVToBQRow.process(rawPDCCollection, successCounter, failureCounter);
    processedCollection.get(CSVToBQRow.successTag).apply(
        String.format("Write Records to BQ"),
        BigQueryIO.writeTableRows().to(bqTable).withSchema(Kafka2BQ.readSchema("/message.json"))
        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));
    processedCollection.get(CSVToBQRow.deadLetterTag).apply(
        String.format("Write failed Records to BQ"),
        BigQueryIO.writeTableRows().to(bqTableError).withSchema(Kafka2BQ.readSchema("/failedmessage.json"))
            .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
            .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));

    pipeline.run();
  }

  static TableSchema readSchema(String fileName) throws Exception {
    InputStream ioStream = Kafka2BQ.class.getResourceAsStream(fileName);
    List<TableFieldSchema> fields = new ArrayList<TableFieldSchema>();
    JsonParser parser = JacksonFactory.getDefaultInstance().createJsonParser(ioStream);
    parser.parseArrayAndClose(fields, TableFieldSchema.class);
    return new TableSchema().setFields(fields);
  }

  static TableReference getTableReference(String projectId, String dataSetId, String bqTableName) {
    TableReference table = new TableReference();
    table.setProjectId(projectId);
    table.setDatasetId(dataSetId);
    table.setTableId(bqTableName);
    return table;
  }

}
