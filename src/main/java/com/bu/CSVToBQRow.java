package com.bu;

import com.google.api.services.bigquery.model.TableRow;
import java.util.UUID;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;

public class CSVToBQRow extends DoFn<String, TableRow> {

  public static TupleTag<TableRow> successTag = new TupleTag<TableRow>() {};
  public static TupleTag<TableRow> deadLetterTag = new TupleTag<TableRow>() {};

  public static String headers = "ID,Card Type Code,Card Type Full Name,Issuing Bank,Card Number,Card Holder's Name,Issue Date,Expiry Date,Billing Date,Card PIN,Credit Limit,Age,SSN,JobTitle,Additional Details";

  public static PCollectionTuple process(PCollection<String> csvLines, Counter successCounter, Counter failureCounter) {
    return csvLines.apply("Parse Topic Data and Extract Information", ParDo
        .of(new DoFn<String, TableRow>() {
      @ProcessElement
      public void processElement(DoFn<String, TableRow>.ProcessContext c) {
        if (!c.element().equals(headers)) {
          try {
            String[] components = c.element().split(",");
            TableRow row = new TableRow()
                .set("Id", Integer.parseInt(components[0].trim()))
                .set("CardTypeCode", components[1].trim())
                .set("CardTypeFullName", components[2].trim())
                .set("IssuingBank", components[3].trim())
                .set("CardNumber", components[4].trim())
                .set("CardHolderName", components[5].trim())
                .set("IssueDate", components[6].trim())
                .set("ExpiryDate", components[7].trim())
                .set("BillingDate", components[8].trim())
                .set("CardPIN", components[9].trim())
                .set("CreditLimit", components[10].trim())
                .set("Age", Integer.parseInt(components[11].trim()))
                .set("SSN", components[12].trim())
                .set("JobTitle", components[13].trim())
                .set("AdditionalDetails", components[14].trim());
            c.output(successTag, row);
            successCounter.inc();
          } catch (Exception exception) {
            failureCounter.inc();
            String corelationId = UUID.randomUUID().toString();
            TableRow failedRecord = new TableRow();
            failedRecord.set("CorrelationId", corelationId);
            failedRecord.set("Data", c.element());
            failedRecord.set("ErrorMessage", exception.getMessage());
            failedRecord.set("ErrorTimestamp", System.currentTimeMillis());
            c.output(deadLetterTag,failedRecord);
          }
        }
      }
    }).withOutputTags(successTag, TupleTagList.of(deadLetterTag)));
  }
}

