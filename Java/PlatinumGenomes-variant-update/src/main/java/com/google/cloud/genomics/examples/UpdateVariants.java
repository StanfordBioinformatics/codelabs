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
import java.util.HashMap;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.api.services.genomics.Genomics;
import com.google.api.services.genomics.model.Variant;
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

  public static final String FAILED_QC_FLAG = "QC";
  
  // TODO: this is currently hardcoded, but if it were useful, the input file could also include the
  // reason(s), why the variant failed QC.
  public static final String FAILED_QC_VALUE = "failed hwe";

  private static final Logger LOG = Logger
      .getLogger(UpdateVariants.class.getName());
  
  /**
   * Pipeline options supported by {@link UpdateVariants}.
   * <p>
   * Inherits standard configuration options for Genomics pipelines.
   */
  private static interface Options extends GenomicsOptions {
    @Description("Path to file(s) from which to read variantIds (local or Google Cloud Storage).")
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
   * The result of this DoFn is the side-effect (modified variants) and emits the count of variants
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
    
    @Override
    public void processElement(ProcessContext context) throws Exception {
      String variantId = context.element().trim(); 
      if (variantId.isEmpty()) {
        return;
      }
 
      // Fetch the variant first so that we can get the current entries, if any. This allows us to
      // preserve the current info entries while adding this new entry.
      Variant variant = genomics.variants().get(variantId).execute();
      if(LOG.isLoggable(Level.FINE)) {
        LOG.fine("About to update: " + variant);
      }
      
      // Some variants may have no info entries.
      if(variant.getInfo() == null) {
        variant.setInfo(new HashMap<String, List<String>>());
      }
      
      // Add the flag to the current set of entries in the variant info field.
      variant.getInfo().put(FAILED_QC_FLAG, Arrays.asList(FAILED_QC_VALUE));
      
      Variant updatedVariant = genomics.variants().update(variantId, variant).execute();
      if(LOG.isLoggable(Level.FINE)) {
        LOG.fine("Updated: " + updatedVariant); 
      }
      
      flaggedVariantCount.addValue(1L);
      
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
