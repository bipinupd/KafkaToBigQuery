package com.bu;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.ValueProvider;

public interface Kafka2BigQueryOptions extends DataflowPipelineOptions {

  @Description("Kafka Brokers (Server:port[Comma Separated])")
  ValueProvider<String> getKafakBootstrapServer();
  void setKafakBootstrapServer(ValueProvider<String> value);

  @Description("Kafka TopicName")
  @Validation.Required
  ValueProvider<String> getTopicName();
  void setTopicName(ValueProvider<String> value);

  @Description("Expected dataset name")
  @Validation.Required
  ValueProvider<String> getDataset();
  void setDataset(ValueProvider<String> value);

  @Description("Expect table name")
  @Validation.Required
  ValueProvider<String> getTableName();
  void setTableName(ValueProvider<String> value);

  @Description("Archive Bucket")
  @Validation.Required
  ValueProvider<String> getArchiveBucket();
  void setArchiveBucket(ValueProvider<String> value);


  @Description("archiveFilenamePrefix")
  @Validation.Required
  ValueProvider<String> getArchiveFilenamePrefix();
  void setArchiveFilenamePrefix(ValueProvider<String> value);

  @Description("numOfShards")
  @Default.Integer(3)
  ValueProvider<Integer> getNumShards();
  void setNumShards(ValueProvider<Integer> value);

}
