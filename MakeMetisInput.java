/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.examples;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableList;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableCell;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.BigQueryIO;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.Default;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.options.Validation;
import com.google.cloud.dataflow.sdk.transforms.Count;
import com.google.cloud.dataflow.sdk.transforms.Sum;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.Aggregator;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.Flatten;
import com.google.cloud.dataflow.sdk.transforms.GroupByKey;
import com.google.cloud.dataflow.sdk.transforms.RemoveDuplicates;
import com.google.cloud.dataflow.sdk.transforms.Combine;
import com.google.cloud.dataflow.sdk.transforms.Filter;
import com.google.cloud.dataflow.sdk.transforms.Combine.KeyedCombineFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.View;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollectionView;
import com.google.cloud.dataflow.sdk.values.PCollectionList;

import com.google.cloud.dataflow.sdk.coders.SerializableCoder;
import com.google.cloud.dataflow.sdk.coders.DefaultCoder;

import org.apache.commons.collections4.set.ListOrderedSet;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.Map;
import java.util.Set;
import java.util.Calendar;
import java.util.Date;
import java.text.SimpleDateFormat;
import java.util.LinkedHashMap;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import info.debatty.java.lsh.LSHMinHash;
import info.debatty.java.lsh.LSH;
import java.util.TreeSet;
import java.util.Arrays;
import java.util.ArrayList;

import java.lang.Math;

public class MakeMetisInput {

  private static final String INPUT = "gs://clickstreams-clustering/input/400K.csv";
  private static final String OUTPUT = "gs://clickstreams-clustering/output/metis.mgraph";
  private static final int BANDS = 20;
  //private static final int BUCKETS = 10000;
  //private static final int VOCAB = 23726;
  private static final int BUCKETS = 50000;
  private static final int VOCAB = 23726;
  private static final int AVG_DURATION = 2000;
  private static final int SIM_THRESHOLD = 70;

 
  public static HashMap similarity(String path1Str, String path2Str, String duration1Str, String duration2Str) {
    // deal with pages encoded vectors
    ArrayList<Integer> path1 = new ArrayList<Integer>();
    for(String s: path1Str.split("\\|")) path1.add(Integer.parseInt(s));
    ArrayList<Integer> path2 = new ArrayList<Integer>();
    for(String s: path2Str.split("\\|")) path2.add(Integer.parseInt(s));
    // deal with duration vectors & calculate total time for each paths
    ArrayList<Integer> duration1 = new ArrayList<Integer>();
    double totDuration1 = 0.0;
    for(String s: duration1Str.split("\\|")){
      duration1.add(Integer.parseInt(s));
      totDuration1 += Double.parseDouble(s);
    }
    duration1.add(AVG_DURATION);
    totDuration1 += Double.valueOf(AVG_DURATION);
    ArrayList<Integer> duration2 = new ArrayList<Integer>();
    double totDuration2 = 0.0;
    for(String s: duration2Str.split("\\|")){
      duration2.add(Integer.parseInt(s));
      totDuration2 += Double.parseDouble(s);
    }
    duration2.add(AVG_DURATION);
    totDuration2 += Double.valueOf(AVG_DURATION);
    // lcs algorithm
    int[][] lengths = new int[path1.size()+1][path2.size()+1];
    for (int i = 0; i < path1.size(); i++)
        for (int j = 0; j < path2.size(); j++)
            if (path1.get(i) == path2.get(j))
                lengths[i+1][j+1] = lengths[i][j] + 1;
            else
                lengths[i+1][j+1] = Math.max(lengths[i+1][j], lengths[i][j+1]);
    ArrayList<Double> durationOnLcs1 = new ArrayList<Double>();
    ArrayList<Double> durationOnLcs2 = new ArrayList<Double>();
    ArrayList<Double> similarityPerPage = new ArrayList<Double>();
    ArrayList<Integer> lcs = new ArrayList<Integer>();
    double totDurationOnLcs1 = 0.0, totDurationOnLcs2 = 0.0, d1, d2;
    for (int x = path1.size(), y = path2.size(); x != 0 && y != 0;) {
        if (lengths[x][y] == lengths[x-1][y])
            x--;
        else if (lengths[x][y] == lengths[x][y-1])
            y--;
        else {
            assert path1.get(x-1) == path2.get(y-1);
            lcs.add(0, path1.get(x-1));
            d1 = (double) duration1.get(x-1);
            d2 = (double) duration2.get(y-1);
            durationOnLcs1.add(0, d1);
            durationOnLcs2.add(0, d2);
            totDurationOnLcs1 += d1;
            totDurationOnLcs2 += d2;
            similarityPerPage.add(0, Math.min(d1, d2)/ Math.max(d1, d2));
            x--;
            y--;
        }
    }
    // calculate similarity between the pair of clickstreams
    double similarity = 0.0;
    for (int i = 0; i < similarityPerPage.size(); i++) similarity += similarityPerPage.get(i);
    similarity = similarity / similarityPerPage.size();
    double importance = Math.sqrt((totDurationOnLcs1/totDuration1) * (totDurationOnLcs2/totDuration2));
    int globalSimilarity = (int) Math.round(similarity * importance * 100.0);
    // prepare output
    HashMap<String, String> out = new HashMap();
    out.put("path1", new String(path1Str));
    out.put("path2", new String(path2Str));
    out.put("duration1", new String(duration1Str));
    out.put("duration2", new String(duration2Str));
    out.put("totDuration1", new String(String.valueOf(totDuration1)));
    out.put("totDuration2", new String(String.valueOf(totDuration2)));
    if(lcs.size() > 0) {
      out.put("totDurationOnLcs1", new String(String.valueOf(totDurationOnLcs1)));
      out.put("totDurationOnLcs2", new String(String.valueOf(totDurationOnLcs2)));
      out.put("importance", new String(String.valueOf(importance)));
      out.put("similarity", new String(String.valueOf(similarity)));
      out.put("globalSimilarity", new String(String.valueOf(globalSimilarity)));
      // turn array lists to strings
      String out1 = "", out2 = "", out3 = "", out4 = "";
      for (int i = 0; i < lcs.size(); i++){
        out1 += lcs.get(i) + "|";
        out2 += durationOnLcs1.get(i) + "|";
        out3 += durationOnLcs2.get(i) + "|";
        out4 += similarityPerPage.get(i) + "|";
      }
      out1 = out1.substring(0, out1.length()-1);
      out2 = out2.substring(0, out2.length()-1);
      out3 = out3.substring(0, out3.length()-1);
      out4 = out4.substring(0, out4.length()-1);
      out.put("lcs", new String(out1));
      out.put("durationOnLcs1", new String(out2));
      out.put("durationOnLcs2", new String(out3));
      out.put("similarityPerPage", new String(out4));
    }
    return out;
  }

  static class YieldPairs extends DoFn<KV<Integer, Iterable<String>>, String> {
    private static final long serialVersionUID = 0;
    @Override
    public void processElement(ProcessContext c){                                                                     
      ListOrderedSet<String> sessions = new ListOrderedSet<String>();
      for(String session: c.element().getValue()) {
        sessions.add(session);
      }
      String[] keySessionDuration1 = new String[3];
      String[] keySessionDuration2 = new String[3];
      String keyPair, pathPair, durationPair;
      for(int i = 0; i < sessions.size(); i++) {
        keySessionDuration1 = sessions.get(i).split("_"); 
        for(int j = i; j < sessions.size(); j++) {
          keySessionDuration2 = sessions.get(j).split("_");
          keyPair = keySessionDuration1[0] + "_" + keySessionDuration2[0];
          pathPair = keySessionDuration1[1] + "_" + keySessionDuration2[1];
          durationPair = keySessionDuration1[2] + "_" + keySessionDuration2[2];
          c.output(keyPair + " " + pathPair + "_" + durationPair);  
        }                                                                      
      }
    }
  }


  static class CalculateSimilarity extends DoFn<String, KV<String, String>> {
    private static final long serialVersionUID = 0;

    private Aggregator<Long> totEdges;

    @Override
    public void startBundle(Context c) {
      totEdges = c.createAggregator("totEdges", new Sum.SumLongFn());
    }

    @Override
    public void processElement(ProcessContext c){
      String[] keys = c.element().split(" ")[0].split("_");
      String[] vals = c.element().split(" ")[1].split("_");
      String path1 = vals[0];
      String path2 = vals[1];
      String duration1 = vals[2];
      String duration2 = vals[3];
      HashMap<String, String> result = similarity(path1, path2, duration1, duration2);
      if(result.get("lcs") != null && 
         Integer.parseInt(result.get("globalSimilarity")) >= SIM_THRESHOLD && 
         ! keys[0].equals(keys[1])) {
        c.output(KV.of(keys[0], keys[1] + ' ' + result.get("globalSimilarity")));
        c.output(KV.of(keys[1], keys[0] + ' ' + result.get("globalSimilarity")));
        totEdges.addValue(1L);
      } else {
        c.output(KV.of(keys[0], ""));
        c.output(KV.of(keys[1], "")); 
      }                                                                     
    }
  }


  static class Concat extends DoFn<KV<String, Iterable<String>>, String> {
    private static final long serialVersionUID = 0;
    @Override
    public void processElement(ProcessContext c){
      String out = "";
      for(String s: c.element().getValue()){
        if(s.length()>0){
          out += s + ' ';
        } 
      }
      c.output(c.element().getKey() + ':' + out);
    }
  }

  private static interface Options extends PipelineOptions {
    
    @Description("Bigquery input table specified as <project_id>:<dataset_id>.<table_prefix>")
    @Default.String(INPUT)
    String getInput();
    void setInput(String value);
    
    @Description("Cloud storage file output for pair similarities")
    @Default.String(OUTPUT)
    String getOutput();
    void setOutput(String value);

    @Description("Number of bands")
    @Default.Integer(BANDS)
    int getNbBands();
    void setNbBands(int value);

    @Description("Number of buckets")
    @Default.Integer(BUCKETS)
    int getNbBuckets();
    void setNbBuckets(int value);

    @Description("Size of the vocabulary")
    @Default.Integer(VOCAB)
    int getSizeVocab();
    void setSizeVocab(int value);

  }


  public static void main(String[] args) throws ParseException{
    
    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    Pipeline p = Pipeline.create(options);

    PCollection<String> clickstreams = p.apply(TextIO.Read.from(options.getInput()));
    
    // trick so that lsh can be used as side input
    LSHMinHash lsh = new LSHMinHash(options.getNbBands(), options.getNbBuckets(), options.getSizeVocab());
    PCollection<LSHMinHash> lshConf = p.apply(Create.of(lsh)).setCoder(SerializableCoder.of(LSHMinHash.class));
    final PCollectionView<LSHMinHash, ?> lshConfView = lshConf.apply(View.<LSHMinHash>asSingleton());

    // seems that we have no choice than passing an anonymous DoFn when we use a side input
    PCollection<KV<Integer, String>> bucketAndSession = clickstreams.apply(ParDo.withSideInputs(lshConfView).of(new DoFn<String, KV<Integer, String>>() {
                                                                             private static final long serialVersionUID = 0; 
                                                                             @Override
                                                                              public void processElement(ProcessContext c){
                                                                                if(! c.element().equals("session,duration,key")){
                                                                                  LSHMinHash lsh = c.sideInput(lshConfView);
                                                                                  String[] vals = c.element().split(",");
                                                                                  String keySessionDuration = vals[2] + "_" + vals[0] + "_" + vals[1];
                                                                                  String[] codes = vals[0].split("\\|");
                                                                                  TreeSet<Integer> set = new TreeSet<Integer>();
                                                                                  for(int i = 0; i < codes.length; i++) {
                                                                                    set.add(Integer.valueOf(codes[i]));
                                                                                  }
                                                                                  int[] signature = lsh.hash(set);
                                                                                  for(int i = 0; i < signature.length; i++) {
                                                                                    c.output(KV.of(signature[i], keySessionDuration));
                                                                                  }
                                                                                }
                                                                              }
                                                                          }));

    PCollection<KV<Integer, Iterable<String>>> allSessionsPerBucket = bucketAndSession.apply(GroupByKey.<Integer, String>create());
    PCollection<String> pairs = allSessionsPerBucket.apply(ParDo.of(new YieldPairs()));
    PCollection<String> uniquePairs = pairs.apply(RemoveDuplicates.<String>create());
    PCollection<KV<String, String>> splitPairs = uniquePairs.apply(ParDo.of(new CalculateSimilarity()));
    PCollection<KV<String, Iterable<String>>> edgesList = splitPairs.apply(GroupByKey.<String, String>create());
    PCollection<String> out = edgesList.apply(ParDo.of(new Concat()));
    out.apply(TextIO.Write.to(options.getOutput()));
    p.run();

  }
}

//http://mvnrepository.com/artifact/org.apache.commons/commons-collections4/4.0
//http://debatty.info/software/java-lsh
//https://maven.apache.org/guides/getting-started/maven-in-five-minutes.html
//http://web.stanford.edu/class/cs345a/slides/05-LSH.pdf
//http://web.stanford.edu/class/cs246/slides/03-lsh.pdf
//mvn exec:java -pl examples -Dexec.mainClass=com.google.cloud.dataflow.examples.ClickstreamClusteringBis -Dexec.args="--project=melodic-metrics-638 --stagingLocation=gs://dataflow-testing/staging --numWorkers=20 --runner=BlockingDataflowPipelineRunner"
//https://cloud.google.com/dataflow/java-sdk/JavaDoc/com/google/cloud/dataflow/sdk/options/DataflowPipelineWorkerPoolOptions#getWorkerMachineType()

//>mvn exec:java -pl examples -Dexec.mainClass=com.google.cloud.dataflow.examples.ClickstreamClusteringQuater -Dexec.args="--project=melodic-metrics-638 --stagingLocation=gs://dataflow-testing/staging --workerMachineType=n1-standard-8 --zone=us-central1-b --numWorkers=25 --runner=DataflowPipelineRunner"