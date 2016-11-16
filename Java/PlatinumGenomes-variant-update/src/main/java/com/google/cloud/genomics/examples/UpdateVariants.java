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

import com.google.api.client.util.BackOff;
import com.google.api.client.util.ExponentialBackOff;
import com.google.api.services.genomics.Genomics;
import com.google.api.services.genomics.model.MergeVariantsRequest;
import com.google.api.services.genomics.model.Variant;
import com.google.api.services.genomics.model.VariantCall;
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
import com.google.cloud.genomics.utils.OfflineAuth;

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
        @Description("Path to file(s) from which to read newline-separated variantIds (local or Google Cloud Storage)." +
                " Each line should be a comma separated list of variantIds, their reason for failure, and optionally" +
                " a call set name to flag specifically.\n" +
                "Example: \n" +
                "CP2Ry8fS3pXI7QESBWNocjE4GKROIMLN4sDr2MX3ywE,hardy_weinberg\n" +
                "CP2Ry8fS3pXI7QESBWNocjE5GMD3BCCTitD_kq-XyCU,titv_by_depth,LP6005038-DNA_A02")
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

        final Aggregator<Long, Long> flaggedVariantCount = createAggregator("Number of variants updated", new Sum.SumLongFn());
        protected Genomics genomics;
        protected OfflineAuth auth;

        public FlagVariant(OfflineAuth auth) {
            super();
            this.auth = auth;

        }


        @Override
        public void startBundle(Context context) throws IOException, GeneralSecurityException {
            genomics = GenomicsFactory.builder().build().fromOfflineAuth(auth);

        }

        // This method aggregates any existing qc tests failed and returns a list that includes the qc test to be added.
        public List getQcValues(Map<String, List<Object>> info, String failedQcValue) {
            List<Object> qcValues = new LinkedList<Object>(Arrays.asList(failedQcValue));
            if (info.containsKey(FAILED_QC_FLAG)) {
                List<Object> values = info.get(FAILED_QC_FLAG);
                //System.out.println("CURRENT VALUES");
                //System.out.println(values);
                for (Object value : values) {
                    if (!qcValues.contains(value)) {
                        qcValues.add(value);
                    }
                }
            }
            //System.out.println("NEW VALUES");
            //System.out.println(qcValues);
            return qcValues;
        }

        public boolean checkExisting(Map<String, List<Object>> info, String failedQcValue) {
            if (info.containsKey(FAILED_QC_FLAG)) {
                List<Object> values = info.get(FAILED_QC_FLAG);
                //System.out.println(values.toString()); //todo remove this
                if (values.contains(failedQcValue)) {
                    return true;
                }
                //for (String value : values) {
                //    if (failedQcValue.equals(value)) {
                //        return true;
                //    }
                //}
            }
            return false;
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

            if (variantId.equals("variant_id")){
                return;
            }

            //System.out.println(variantId);
            //System.out.println(failedQcValue);

            // Fetch the variant first so that we can get the current entries, if any. This allows us to
            // preserve the current info entries while adding this new entry.
            //Variant variant = genomics.variants().get(variantId).execute();


            // Execute through exponential backoff
            ExponentialBackOff backoff = new ExponentialBackOff.Builder().build();
            Variant variant = new Variant();
            Boolean backoff_progress = true;

            while (backoff_progress) {
                try {
                    variant = genomics.variants().get(variantId).execute();
                    backoff_progress = false;
                } catch (Exception e) {
                    if (e.getMessage().startsWith("429 Too Many Requests")) {
                        LOG.warning("Backing-off per: " + e);
                        long backOffMillis = backoff.nextBackOffMillis();
                        if (backOffMillis == BackOff.STOP) {
                            throw e;
                        }
                        Thread.sleep(backOffMillis);
                    } else {
                        throw e;
                    }
                }
            }



            //System.out.println(variant.getCalls());

            //Variant test = genomics.var
            if(LOG.isLoggable(Level.FINE)) {
                LOG.fine("About to update: " + variant);
            }



            // Some variants may have no info entries.
            if(variant.getInfo() == null) {
                variant.setInfo(new HashMap<String, List<Object>>());
            }

            // Update a variant for all call sets
            if(callSetName.isEmpty()) {
                // Check if there is already a qc value
                Map<String, List<Object>> info = variant.getInfo();
                //System.out.println(info);

                // Check existing
                boolean exists = checkExisting(info, failedQcValue);
                if (exists) {
                    flaggedVariantCount.addValue(1L);
                    context.output(1);
                    return;
                }

                List<Object> qcValues = getQcValues(info, failedQcValue);

                // Add the flag to the current set of entries in the variant info field.
                variant.getInfo().put(FAILED_QC_FLAG, qcValues);


                // Execute through exponential backoff
                ExponentialBackOff backoff2 = new ExponentialBackOff.Builder().build();
                backoff_progress = true;

                while (backoff_progress) {
                    try {

                        Variant updatedVariant = genomics.variants().patch(variantId, variant).execute();
                        flaggedVariantCount.addValue(1L);
                        backoff_progress = false;
                    } catch (Exception e) {
                        if (e.getMessage().startsWith("429 Too Many Requests")) {
                            LOG.warning("Backing-off per: " + e);
                            long backOffMillis = backoff2.nextBackOffMillis();
                            if (backOffMillis == BackOff.STOP) {
                                throw e;
                            }
                            Thread.sleep(backOffMillis);
                        } else {
                            throw e;
                        }
                    }
                }






                // PRE EXPONENTIAL BACKOFF CODE //

                //// Execute the update
                //Variant updatedVariant = genomics.variants().patch(variantId, variant).execute();
                ////Variant updated = genomics.variants().p
                //if(LOG.isLoggable(Level.FINE)) {
                //    LOG.fine("Updated: " + updatedVariant);
                //}
//
                //// Add to the total count
                //flaggedVariantCount.addValue(1L);
            }

            // If a call set name is found we want to update the call info within the variant rather than the variant info
            else {
                // Get the call data associated with a variant
                List<VariantCall> calls = variant.getCalls();

                // Loop through all calls looking for matching call set names
                for (VariantCall call : calls) {
                    String callName = call.getCallSetName();
                    System.out.println(callName);
                    System.out.println(callSetName);
                    if (callName.equals(callSetName)) {
                        System.out.println("Match found");
                        Map<String, List<Object>> info = call.getInfo();

                        // Check existing
                        boolean exists = checkExisting(info, failedQcValue);
                        if (exists) {
                            flaggedVariantCount.addValue(1L);
                            context.output(1);
                            return;
                        }
                        System.out.print(failedQcValue);
                        List<Object> qcValues = getQcValues(info, failedQcValue);
                        call.getInfo().put(FAILED_QC_FLAG, qcValues);
                        System.out.println(call.toPrettyString());

                        break;
                    }
                }
                System.out.println(variant.toPrettyString());

                variant.setId(null);
                String variantSetId = variant.getVariantSetId();

                MergeVariantsRequest request = new MergeVariantsRequest();
                request.setVariants(Arrays.asList(variant));
                request.setVariantSetId(variantSetId);

                //System.out.println(request.toPrettyString());



                // Execute through exponential backoff
                ExponentialBackOff backoff3 = new ExponentialBackOff.Builder().build();
                backoff_progress = true;

                while (backoff_progress) {
                    try {
                        genomics.variants().merge(request).execute();
                        flaggedVariantCount.addValue(1L);
                        backoff_progress = false;
                    } catch (Exception e) {
                        if (e.getMessage().startsWith("429 Too Many Requests")) {
                            LOG.warning("Backing-off per: " + e);
                            long backOffMillis = backoff3.nextBackOffMillis();
                            if (backOffMillis == BackOff.STOP) {
                                throw e;
                            }
                            Thread.sleep(backOffMillis);
                        } else {
                            throw e;
                        }
                    }
                }


                // PRE EXPONENTIAL BACKOFF CODE //

  //               //genomics.variants().merge(request).execute();
//
                //
                //
                // // Add to the total count
                //flaggedVariantCount.addValue(1L);
            }

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
        //GenomicsOptions.Methods.validateOptions(options);
        OfflineAuth auth = GenomicsOptions.Methods.getGenomicsAuth(options);

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


