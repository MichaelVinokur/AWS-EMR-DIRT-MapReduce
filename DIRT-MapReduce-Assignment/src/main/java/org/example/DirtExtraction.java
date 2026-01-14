package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.tartarus.snowball.ext.EnglishStemmer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.*;


public class DirtExtraction {

    //cease   cease/VB/ccomp/0 for/IN/prep/1 an/DT/det/4 instant/NN/pobj/2    56	1834,2	1835,1	1856,1	1863,1	1871,1	1872,1	1874,1	1875,3	1880,2	1883,2	1889,1	1904,7	1905,2	1915,5	1918,1	1961,1	1963,5	1973,2	1975,1	1977,1	1981,2	1987,2	1988,1	1989,1	1991,1	1996,5	2000,1	2008,2
    public static class DirtMapper extends Mapper<LongWritable, Text, Text, Text> {
        private final EnglishStemmer stemmer = new EnglishStemmer();
        private final Text outKey = new Text();
        private final Text outValue = new Text();

        private final Set<String> auxiliaryVerbs = new HashSet<>(Arrays.asList(
                "be", "is", "am", "are", "was", "were", "been", "being",
                "have", "has", "had", "do", "does", "did"
        )); // for filtering auxilery verbs

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split("\t");
            if (tokens.length < 3) return;

            String headWord = tokens[0];
            String stemmedHead = applyStemming(headWord); //checks if part of auxileryVerbs and applying stemmer.
            if (stemmedHead == null) return;

            int freq;
            try { freq = Integer.parseInt(tokens[2].trim()); }
            catch (Exception e) { return; }

            boolean headIsVerb = false;
            String prep = null;      // e.g. "for" - IN
            String xNoun = null; // subject = נושא
            String yNoun = null; // object = מושא

            String[] syntacticNgram = tokens[1].split("\\s+"); // cease/VB/ccomp/0  for/IN/prep/1  some/DT/det/4  time/NN/pobj/2
            for(String s: syntacticNgram){
                String [] sBreak = s.split("/");
                if (sBreak.length < 2) continue;
                String word = sBreak[0];
                String pos = sBreak[1];
                String dep="";
                if(sBreak.length>2) {
                    dep = sBreak[2];
                }

                if(word.equals(headWord)){
                    if(pos.startsWith("VB")){ headIsVerb=true;}
                    else return; // if head not verb ignore
                }
                if(pos.startsWith("NN") || pos.startsWith("NNS") || pos.startsWith("NNP") || pos.startsWith("NNPS")){
                    String stem = applyStemming(word);
                    if (stem == null) continue;

                    if (dep.equals("nsubj") && xNoun == null) {
                        xNoun = stem;
                    }
                    if ((dep.equals("dobj") || dep.equals("pobj")) && yNoun == null) {
                        yNoun = stem;
                    }
                }
                if (prep == null && (pos.equals("IN") || pos.equals("TO"))){
                    prep = word.toLowerCase();
                }
            }
            if (!headIsVerb) return;
            if (yNoun == null) return;
            String p;

            String pXY = (prep != null)
                    ? "X " + stemmedHead + " " + prep + " Y"
                    : "X " + stemmedHead + " Y";

            if (xNoun == null) {
                // If you insist on keeping Y-only cases, we still use the same template string
                emitCounts(context, pXY, "Y", yNoun, freq);
            } else {
                emitCounts(context, pXY, "X", xNoun, freq);
                emitCounts(context, pXY, "Y", yNoun, freq);
            }
        }

        private String applyStemming(String word) {
            String lower = word.toLowerCase().trim();
            if (auxiliaryVerbs.contains(lower)) return null; // סינון פעלים עוזרים

            stemmer.setCurrent(lower);
            if (stemmer.stem()) {
                return stemmer.getCurrent();
            }
            return lower;
        }

        private void emitCounts(Context ctx, String p, String slot, String w, int f) throws IOException, InterruptedException {
            // (p, slot, w)
            outKey.set(p + "\t" + slot + "\t" + w);
            outValue.set(Integer.toString(f));
            ctx.write(outKey, outValue);

            // (p, slot, *)
            outKey.set(p + "\t" + slot + "\t" + "*");
            ctx.write(outKey, outValue);

            // (*, slot, w)
            outKey.set("*" + "\t" + slot + "\t" + w);
            ctx.write(outKey, outValue);

            // (TOTAL, slot)
            outKey.set("TOTAL" + "\t" + slot);
            ctx.write(outKey, outValue);
        }
    }

    // --- 2. ה-REDUCER: סכימת מופעים לחישוב MI בהמשך ---
    public static class DirtReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            long sum = 0;
            for (Text val : values) {
                sum += Long.parseLong(val.toString());
            }
            context.write(key, new Text(Long.toString(sum)));
        }
    }


    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: DirtExtraction <input path> <output path>");
            System.exit(-1);
        }
        System.setProperty("hadoop.home.dir", "C:/src/hadoop/hadoop-3.3.5/winutils-master/hadoop-3.3.5");
        Configuration conf = new Configuration();
        String tmpDir = "C:/src/DIRT-MapReduce-Assignment/DIRT-MapReduce-Assignment/src/main/hadoop_tmp";
        conf.set("hadoop.tmp.dir", tmpDir);
        conf.set("mapreduce.jobtracker.staging.root.dir", tmpDir + "/staging");
        conf.set("mapreduce.cluster.local.dir", tmpDir + "/local");
        conf.set("mapreduce.framework.name", "local");
        conf.set("fs.defaultFS", "file:///");
        conf.set("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem");
        conf.set("mapreduce.jobtracker.address", "local");
        System.out.println("EXISTS=" + new java.io.File(args[0]).exists());
        System.out.println("SIZE=" + new java.io.File(args[0]).length());
        Job job = Job.getInstance(conf, "DIRT Extraction Job");
        job.addCacheFile(new Path("file:///C://src//DIRT-MapReduce-Assignment//DIRT-MapReduce-Assignment//src//main//globals.txt").toUri());
        job.setJarByClass(DirtExtraction.class);
        job.setMapperClass(DirtMapper.class);
        job.setReducerClass(DirtReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setInputFormatClass(org.apache.hadoop.mapreduce.lib.input.TextInputFormat.class);
        System.out.println("INPUT=" + args[0]);
        System.out.println("OUTPUT=" + args[1]);

        try {
            boolean ok = job.waitForCompletion(true);
            System.out.println("MAP_CALLED=" + job.getCounters()
                    .findCounter("DEBUG","MAP_CALLED").getValue());
            if (!ok) throw new RuntimeException("Job failed");
        } catch (Exception e) {
            e.printStackTrace();   // <- THIS is the point
            throw e;
        }
    }
}
























