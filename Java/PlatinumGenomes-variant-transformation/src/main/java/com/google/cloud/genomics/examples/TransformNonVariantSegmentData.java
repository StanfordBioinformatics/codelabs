/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.genomics.examples;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.logging.Logger;

import com.google.api.client.repackaged.com.google.common.base.Preconditions;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.api.services.genomics.model.Call;
import com.google.api.services.genomics.model.SearchVariantsRequest;
import com.google.api.services.genomics.model.Variant;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.BigQueryIO;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.options.Validation;
import com.google.cloud.dataflow.sdk.transforms.Aggregator;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.Sum;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.genomics.dataflow.functions.JoinNonVariantSegmentsWithVariants;
import com.google.cloud.genomics.dataflow.utils.DataflowWorkarounds;
import com.google.cloud.genomics.dataflow.utils.GenomicsDatasetOptions;
import com.google.cloud.genomics.dataflow.utils.GenomicsOptions;
import com.google.cloud.genomics.utils.Contig.SexChromosomeFilter;
import com.google.cloud.genomics.utils.GenomicsFactory;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimaps;

/**
 * Sample pipeline that transforms data with non-variant segments (such as data that was in source
 * format Genome VCF (gVCF) or Complete Genomics) to variant-only data with calls from
 * non-variant-segments merged into the variants with which they overlap. The resultant data is
 * emitted to a BigQuery table.
 *
 * This is currently done only for SNP variants. Indels and structural variants are left as-is.
 *
 * The data source is the Google Genomics Variants API. The data sink is BigQuery
 *
 * <p>
 * The sample could be expanded upon to:
 * <ol>
 * <li>emit additional fields from the variants and calls
 * <li>perform additional data munging
 * <li>write data to a different sink such as Google Cloud Storage or Google Datastore
 * </ol>
 */
public class TransformNonVariantSegmentData {

  private static final Logger LOG = Logger
      .getLogger(TransformNonVariantSegmentData.class.getName());

  public static final String[] CALL_INFO_FIELDS = {"QUAL", "DP", "FILTER", "GQ"};
  public static final String HAS_AMBIGUOUS_CALLS_FIELD = "ambiguousCalls";

  // TEMPORARY HACK: filter out known bad callsets.
  public static final ImmutableSet<String> GENOMES_TO_SKIP = ImmutableSet.of(
      "LP6005038-DNA_C02",
      "LP6005038-DNA_D03",
      "LP6005051-DNA_D04",
      "LP6005051-DNA_D09",
      "LP6005144-DNA_A02",
      "LP6005144-DNA_D03",
      "LP6005144-DNA_E04",
      "LP6005144-DNA_G07",
      "LP6005144-DNA_G08",
      "LP6005243-DNA_A08",
      "LP6005243-DNA_A12",
      "LP6005243-DNA_B12",
      "LP6005243-DNA_C03",
      "LP6005243-DNA_C07",
      "LP6005243-DNA_C12",
      "LP6005243-DNA_D12",
      "LP6005243-DNA_E12",
      "LP6005243-DNA_F11",
      "LP6005243-DNA_F12",
      "LP6005243-DNA_G11",
      "LP6005243-DNA_G12",
      "LP6005243-DNA_H03",
      "LP6005243-DNA_H11",
      "LP6005243-DNA_H12",
      "LP6005692-DNA_A04",
      "LP6005692-DNA_D05",
      "LP6005692-DNA_E02",
      "LP6005692-DNA_E10",
      "LP6005692-DNA_F12",
      "LP6005692-DNA_G09",
      "LP6005692-DNA_G12",
      "LP6005692-DNA_H01",
      "LP6005692-DNA_H12",
      "LP6005693-DNA_B03",
      "LP6005693-DNA_C03",
      "LP6005693-DNA_D03",
      "LP6005693-DNA_H01",
      "LP6005695-DNA_A01");

  /**
   * Options supported by {@link TransformNonVariantSegmentData}.
   * <p>
   * Inherits standard configuration options for Genomics pipelines and datasets.
   */
  private static interface Options extends GenomicsDatasetOptions {
    @Description("BigQuery table to write to, specified as "
        + "<project_id>:<dataset_id>.<table_id>. The dataset must already exist.")
    @Validation.Required
    String getOutputTable();

    void setOutputTable(String value);
  }

  /**
   * Construct the table schema for the output table.
   *
   * @return The schema for the destination table.
   */
  private static TableSchema getTableSchema() {
    List<TableFieldSchema> callFields = new ArrayList<>();
    callFields.add(new TableFieldSchema().setName("call_set_name").setType("STRING"));
    callFields.add(new TableFieldSchema().setName("phaseset").setType("STRING"));
    callFields.add(new TableFieldSchema().setName("genotype").setType("INTEGER")
        .setMode("REPEATED"));
    callFields.add(new TableFieldSchema().setName("genotype_likelihood").setType("FLOAT")
        .setMode("REPEATED"));
    callFields.add(new TableFieldSchema().setName("AD").setType("INTEGER").setMode("REPEATED"));
    callFields.add(new TableFieldSchema().setName("DP").setType("INTEGER"));
    callFields.add(new TableFieldSchema().setName("FILTER").setType("STRING"));
    callFields.add(new TableFieldSchema().setName("GQ").setType("INTEGER"));
    callFields.add(new TableFieldSchema().setName("QUAL").setType("FLOAT"));

    List<TableFieldSchema> fields = new ArrayList<>();
    fields.add(new TableFieldSchema().setName("variant_id").setType("STRING"));
    fields.add(new TableFieldSchema().setName("reference_name").setType("STRING"));
    fields.add(new TableFieldSchema().setName("start").setType("INTEGER"));
    fields.add(new TableFieldSchema().setName("end").setType("INTEGER"));
    fields.add(new TableFieldSchema().setName("reference_bases").setType("STRING"));
    fields.add(new TableFieldSchema().setName("alternate_bases").setType("STRING")
        .setMode("REPEATED"));
    fields.add(new TableFieldSchema().setName("names").setType("STRING").setMode("REPEATED"));
    fields.add(new TableFieldSchema().setName("filter").setType("STRING").setMode("REPEATED"));
    fields.add(new TableFieldSchema().setName("quality").setType("FLOAT"));
    fields.add(new TableFieldSchema().setName(HAS_AMBIGUOUS_CALLS_FIELD).setType("BOOLEAN"));
    fields.add(new TableFieldSchema().setName("call").setType("RECORD").setMode("REPEATED")
        .setFields(callFields));

    return new TableSchema().setFields(fields);
  }

  /**
   * Prepare the data for writing to BigQuery by building a TableRow object containing data from the
   * variant mapped onto the schema to be used for the destination table.
   */
  static class FormatVariantsFn extends DoFn<Variant, TableRow> {
    @Override
    public void processElement(ProcessContext c) {
      Variant v = c.element();

      List<TableRow> calls = new ArrayList<>();
      for (Call call : v.getCalls()) {
        TableRow row =
            new TableRow()
                .set("call_set_name", call.getCallSetName())
                .set("phaseset", call.getPhaseset())
                .set("genotype",
                    (call.getGenotype() == null) ? new ArrayList<String>() : call.getGenotype())
                .set(
                    "genotype_likelihood",
                    (call.getGenotypeLikelihood() == null) ? new ArrayList<String>() : call
                        .getGenotypeLikelihood());

        Map<String, List<String>> info = call.getInfo();
        row.set("AD", (info.containsKey("AD")) ? info.get("AD") : new ArrayList<String>());
        for (String callInfoField : CALL_INFO_FIELDS) {
          if (info.containsKey(callInfoField)) {
            row.set(callInfoField, info.get(callInfoField).get(0));
          }
        }
        calls.add(row);
      }

      TableRow row =
          new TableRow()
              .set("variant_id", v.getId())
              .set("reference_name", v.getReferenceName())
              .set("start", v.getStart())
              .set("end", v.getEnd())
              .set("reference_bases", v.getReferenceBases())
              .set("alternate_bases",
                  (v.getAlternateBases() == null) ? new ArrayList<String>() : v.getAlternateBases())
              .set("names", (v.getNames() == null) ? new ArrayList<String>() : v.getNames())
              .set("filter", (v.getFilter() == null) ? new ArrayList<String>() : v.getFilter())
              .set("quality", v.getQuality()).set("call", calls)
              .set(HAS_AMBIGUOUS_CALLS_FIELD, v.getInfo().get(HAS_AMBIGUOUS_CALLS_FIELD).get(0));

      c.output(row);
    }
  }

  public static final class FlagVariantsWithAmbiguousCallsFn extends DoFn<Variant, Variant> {

    Aggregator<Long> variantsWithAmbiguousCallsCount;

    @Override
    public void startBundle(Context c) {
      variantsWithAmbiguousCallsCount =
          c.createAggregator("Number of variants containing ambiguous calls", new Sum.SumLongFn());
    }

    @Override
    public void processElement(ProcessContext context) {
      Variant variant = context.element();

      if(null == variant.getCalls() || variant.getCalls().isEmpty()) {
        return;
      }

      // TEMPORARY HACK: filter out known bad callsets.
      List<Call> filteredCalls = Lists.newArrayList(Iterables.filter(variant.getCalls(), new Predicate<Call>() {
        @Override
        public boolean apply(Call call) {
          if (GENOMES_TO_SKIP.contains(call.getCallSetName())) {
            return false;
          }
          return true;
        }
      }));
      if(filteredCalls.isEmpty()) {
        return;
      }
      variant.setCalls(filteredCalls);

      // Gather calls together for the same callSetName.
      ListMultimap<String, Call> indexedCalls =
          Multimaps.index(variant.getCalls(), new Function<Call, String>() {
            @Override
            public String apply(final Call c) {
              return c.getCallSetName();
            }
          });

      // Flag and count variants with multiple calls per callSetName.
      boolean isAmbiguous = false;
      for (Entry<String, Collection<Call>> entry : indexedCalls.asMap().entrySet()) {
        if (1 < entry.getValue().size()) {
          LOG.warning("Variant " + variant.getId() + " contains ambiguous calls for "
              + entry.getValue().iterator().next().getCallSetName());
          isAmbiguous = true;
        }
      }
      if (isAmbiguous) {
        variantsWithAmbiguousCallsCount.addValue(1l);
      }

      if(variant.getInfo() == null) {
        variant.setInfo(new HashMap<String, List<String>>());
      }
      variant.getInfo().put(HAS_AMBIGUOUS_CALLS_FIELD, Arrays.asList(Boolean.toString(isAmbiguous)));

      context.output(variant);
    }
  }

  public static void main(String[] args) throws IOException, GeneralSecurityException {
    // Register the options so that they show up via --help
    PipelineOptionsFactory.register(TransformNonVariantSegmentData.Options.class);
    TransformNonVariantSegmentData.Options options =
        PipelineOptionsFactory.fromArgs(args).withValidation()
            .as(TransformNonVariantSegmentData.Options.class);
    // Option validation is not yet automatic, we make an explicit call here.
    GenomicsDatasetOptions.Methods.validateOptions(options);

    Preconditions.checkState(options.getHasNonVariantSegments(),
        "This job is only valid for data containing non-variant segments. "
            + "Set the --hasNonVariantSegments command line option accordingly.");

    GenomicsFactory.OfflineAuth auth = GenomicsOptions.Methods.getGenomicsAuth(options);
    List<SearchVariantsRequest> requests =
        GenomicsDatasetOptions.Methods.getVariantRequests(options, auth,
            SexChromosomeFilter.INCLUDE_XY);

    Pipeline p = Pipeline.create(options);
    DataflowWorkarounds.registerGenomicsCoders(p);

    PCollection<SearchVariantsRequest> input = p.begin().apply(Create.of(requests));

    // Now we have a collection of data with non-variant segments omitted but calls from overlapping
    // non-variant segments added to SNPs.
    PCollection<Variant> variants =
        JoinNonVariantSegmentsWithVariants.joinVariantsTransform(input, auth);

    // If we happen to have any variants in this dataset with ambiguous calls for a sample, flag those.
    PCollection<Variant> flaggedVariants = variants.apply(ParDo.of(new FlagVariantsWithAmbiguousCallsFn()));

    flaggedVariants.apply(ParDo.of(new FormatVariantsFn())).apply(
        BigQueryIO.Write.to(options.getOutputTable()).withSchema(getTableSchema())
            .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
            .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE));

    p.run();
  }
}
