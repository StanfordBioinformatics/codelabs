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
import java.util.Arrays;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.LinkedList;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.api.services.genomics.Genomics;
import com.google.api.services.genomics.model.Variant;
import com.google.api.services.genomics.model.Call;
import com.google.api.services.genomics.model.MergeVariantsRequest;
import com.google.api.services.genomics.Genomics.Variantsets.MergeVariants;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.options.Validation;
import com.google.cloud.dataflow.sdk.transforms.Aggregator;
import com.google.cloud.dataflow.sdk.transforms.Combine;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.Sum;
import com.google.cloud.genomics.dataflow.utils.GenomicsOptions;
import com.google.cloud.genomics.utils.GenomicsFactory;


/**
 * Sample pipeline that updates particular variants identified in the input file(s).
 */
public class UpdateVariants {

  // QC tests will be added to this column.  Used for both the variant level and the sample level
  public static final String FAILED_QC_FLAG = "QC";

  private static final Logger LOG = Logger
      .getLogger(UpdateVariants.class.getName());

  /**
   * Pipeline options supported by {@link UpdateVariants}.
   * <p>
   * Inherits standard configuration options for Genomics pipelines.
   */
  private static interface Options extends GenomicsOptions {
    @Description("Path to file(s) from which to read newline-separated variantIds (local or Google Cloud Storage).")
    @Validation.Required
    String getInput();

    void setInput(String value);

    @Description("Path to Google Cloud Storage file(s) to which which to write the count of variants flagged (local or Google Cloud Storage).")
    @Validation.Required
    String getOutput();

    void setOutput(String value);
  }

  /**
   * Given a variantId, modify the variant to add a particular key/value pair to the set of entries
   * in the variant info field.
   *
   * The result of this DoFn is the side-effect (modified variants) and the count of variants
   * modified.
   */
  static class FlagVariant extends DoFn<String, Integer> {

    protected final GenomicsFactory.OfflineAuth auth;
    protected Aggregator<Long> flaggedVariantCount;
    protected Genomics genomics;

    /**
     * @param flaggedVariantCount
     * @param genomics
     * @throws IOException
     * @throws GeneralSecurityException
     */
    public FlagVariant(GenomicsFactory.OfflineAuth auth) {
      super();
      this.auth = auth;
    }


    @Override
    public void startBundle(Context context) throws IOException, GeneralSecurityException {
      flaggedVariantCount =
          context.createAggregator("Number of variants flagged", new Sum.SumLongFn());

      GenomicsFactory factory = auth.getDefaultFactory();
      genomics = auth.getGenomics(factory);
    }

    // This method aggregates any existing qc tests failed and returns a list that includes the qc test to be added.
    public List getQcValues(Map<String, List<String>> info, String failedQcValue) {
      List<String> qcValues = new LinkedList<String>(Arrays.asList(failedQcValue));
      if (info.containsValue(FAILED_QC_FLAG)) {
        List<String> values = info.get(FAILED_QC_FLAG);
        for (String value : values) {
          if (!qcValues.contains(value)) {
            qcValues.add(value);
          }
        }
      }
      return qcValues;
    }


    @Override
    public void processElement(ProcessContext context) throws Exception {
      String line = context.element().trim();
      // Split line by commas and set variables
      // Input should look like this:
      // variantId,failure_reason,call_set_name (optional)
      String[] fields = line.split(",", -1);
      String variantId = fields[0];
      String failedQcValue = fields[1];

      String callSetName = "";
      if (fields.length > 2) {
        callSetName = fields[2];
      }

      if (variantId.isEmpty()) {
        return;
      }

      // Fetch the variant first so that we can get the current entries, if any. This allows us to
      // preserve the current info entries while adding this new entry.
      Variant variant = genomics.variants().get(variantId).execute();
      if(LOG.isLoggable(Level.FINE)) {
        LOG.fine("About to update: " + variant);
      }

      String variantSetId = variant.getVariantSetId();

      List<String> qcValues = new LinkedList<String>(Arrays.asList(failedQcValue));

      // Some variants may have no info entries.
      if(variant.getInfo() == null) {
        variant.setInfo(new HashMap<String, List<String>>());
      }

      // Update a variant for all call sets
      if(callSetName.isEmpty()) {
        // Check if there is already a qc value
        Map<String, List<String>> info = variant.getInfo();
        qcValues = getQcValues(info, failedQcValue);

        // Add the flag to the current set of entries in the variant info field.
        variant.getInfo().put(FAILED_QC_FLAG, qcValues);

        // Execute the update
        Variant updatedVariant = genomics.variants().update(variantId, variant).execute();
        if(LOG.isLoggable(Level.FINE)) {
          LOG.fine("Updated: " + updatedVariant);
        }

        // Add to the total count
        flaggedVariantCount.addValue(1L);
      }

      // If a call set name is found we want to update the call info within the variant rather than the variant info
      else {
        // Get the call data associated with a variant
        List<Call> calls = variant.getCalls();
        // Loop through all calls looking for matching call set names
        for (Call call : calls) {
          String callName = call.getCallSetName();
          if (callName.equals(callSetName)) {
            Map<String, List<String>> info = call.getInfo();
            qcValues = getQcValues(info, failedQcValue);
            call.getInfo().put(FAILED_QC_FLAG, qcValues);
          }
        }

        variant.setId(null);
        MergeVariantsRequest request = new MergeVariantsRequest();
        request.setVariants(Arrays.asList(variant));
        genomics.variantsets().mergeVariants(variantSetId, request).execute();

      }

      // TODO: would any other output be more useful?
      context.output(1);
    }
  }

  public static void main(String[] args) throws IOException, GeneralSecurityException {
    // Register the options so that they show up via --help.
    PipelineOptionsFactory.register(UpdateVariants.Options.class);
    UpdateVariants.Options options =
        PipelineOptionsFactory.fromArgs(args).withValidation()
            .as(UpdateVariants.Options.class);

    // Option validation is not yet automatic, we make an explicit call here.
    GenomicsOptions.Methods.validateOptions(options);
    GenomicsFactory.OfflineAuth auth =
          GenomicsOptions.Methods.getGenomicsAuth(options);

    Pipeline p = Pipeline.create(options);

    p.apply(TextIO.Read.named("ReadLines").from(options.getInput()))
            .apply(ParDo.of(new FlagVariant(auth)))
            .apply(Combine.globally(new Sum.SumIntegerFn()))
            .apply(ParDo.of(new DoFn<Integer, String>() {
              @Override
              public void processElement(DoFn<Integer, String>.ProcessContext c) throws Exception {
                c.output(String.valueOf(c.element()));
              }
            }).named("toString"))
      .apply(TextIO.Write.named("WriteCount").to(options.getOutput()));

    p.run();
  }
}
