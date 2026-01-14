package org.example;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.tartarus.snowball.ext.EnglishStemmer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.*;

public class SimilarityComputation {

    //SECTION: job3
    public static class SimMapper extends Mapper<LongWritable, Text, Text, Text> {
        private final Set<String> testPaths = new HashSet<>();
        private final Text outK = new Text();
        private final Text outV = new Text();
        private static final Set<String> PREPS = new HashSet<>(Arrays.asList(
                "in","on","at","by","for","from","with","to","into","over","under","of","as",
                "during","after","before","through","via","against","upon"
        ));
        private final EnglishStemmer stemmer = new EnglishStemmer();

        @Override
        protected void setup(Mapper.Context context) throws IOException {
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles == null) return;

            FileSystem fs = FileSystem.get(context.getConfiguration());

            for (URI uri : cacheFiles) {
                Path path = new Path(uri.toString());
                try (BufferedReader br =
                             new BufferedReader(new InputStreamReader(fs.open(path)))) {

                    String line;
                    while ((line = br.readLine()) != null) {
                        String[] parts = line.split("\t");
                        if (parts.length >= 2) {
                            String p1 = stemTestPath(parts[0]);
                            String p2 = stemTestPath(parts[1]);
                            if (p1 != null) testPaths.add(p1);
                            if (p2 != null) testPaths.add(p2);
                        }
                    }
                }
            }
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) return;

            String[] parts = line.split("\t");
            if (parts.length != 4) return;

            String p = parts[0];
            if (!testPaths.contains(p)) return; //path not in TestPAths

            String slot = parts[1];
            String w = parts[2];
            String mi = parts[3];

            outK.set(slot + "\t" + w);
            outV.set(p + "\t" + mi);
            context.write(outK, outV);
        }
        private String stem(String w) {
            w = w.toLowerCase().trim();
            stemmer.setCurrent(w);
            stemmer.stem();
            return stemmer.getCurrent();
        }

        private String stemTestPath(String template) {
            if (template == null) return null;
            template = template.trim();
            if (template.isEmpty()) return null;

            String[] toks = template.split("\\s+");
            if (toks.length != 3 && toks.length != 4) return null;

            String leftVar = toks[0];
            String rightVar = toks[toks.length - 1];

            if (!(leftVar.equals("X") || leftVar.equals("Y"))) return null;
            if (!(rightVar.equals("X") || rightVar.equals("Y"))) return null;

            String verbStem = stem(toks[1]);

            String prep = null;
            if (toks.length == 4) {
                prep = toks[2].toLowerCase().trim();
                if (!PREPS.contains(prep)) return null;
            }

            // Normalize: flip Y...X into X...Y (to match corpus extraction)
            boolean flip = leftVar.equals("Y") && rightVar.equals("X");
            String outLeft = flip ? "X" : leftVar;
            String outRight = flip ? "Y" : rightVar;

            if (prep == null) {
                return outLeft + " " + verbStem + " " + outRight;
            } else {
                return outLeft + " " + verbStem + " " + prep + " " + outRight;
            }
        }
    }

    public static class SimReducer extends Reducer<Text, Text, Text, Text> {

        private final Set<String> testPairs = new HashSet<>();
        private final Text outK = new Text();
        private final Text outV = new Text();
        private static final Set<String> PREPS = new HashSet<>(Arrays.asList(
                "in","on","at","by","for","from","with","to","into","over","under","of","as",
                "during","after","before","through","via","against","upon"
        ));
        private final EnglishStemmer stemmer = new EnglishStemmer();

        @Override
        protected void setup(Context context) throws IOException {
            // Load positive + negative pairs from cache
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles == null) return;

            FileSystem fs = FileSystem.get(context.getConfiguration());

            for (URI uri : cacheFiles) {
                try (BufferedReader br = new BufferedReader(
                        new InputStreamReader(fs.open(new Path(uri))))) {

                    String line;
                    while ((line = br.readLine()) != null) {
                        String[] p = line.split("\t");
                        if (p.length < 2) continue;

                        String p1 = templateToP(p[0]);
                        String p2 = templateToP(p[1]);
                        if (p1 == null || p2 == null) continue;

                        String pair = (p1.compareTo(p2) <= 0) ? p1 + "\t" + p2 : p2 + "\t" + p1;
                        testPairs.add(pair);
                    }
                }
            }
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            List<String> paths = new ArrayList<>();
            List<Double> mis = new ArrayList<>();

            for (Text v : values) {
                String[] parts = v.toString().split("\t");
                if (parts.length != 2) continue;
                paths.add(parts[0]);
                try {
                    mis.add(Double.parseDouble(parts[1]));
                } catch (Exception e) {
                    mis.add(0.0);
                }
            }

            // key is "slot\tw" (from mapper)
            String[] kw = key.toString().split("\t", 2);
            if (kw.length != 2) return;
            String slot = kw[0];

            int n = paths.size();
            if (n <= 1) return;

            // safety guard
            if (n > 5000) return;

            for (int i = 0; i < n; i++) {
                for (int j = i + 1; j < n; j++) {
                    String p1 = paths.get(i);
                    String p2 = paths.get(j);

                    String pair = (p1.compareTo(p2) <= 0) ? p1 + "\t" + p2 : p2 + "\t" + p1;
                    if (!testPairs.contains(pair)) continue;

                    // shared-word contribution for this (slot,w)
                    double contrib = mis.get(i) + mis.get(j);   // <-- important double contrib = Math.min(mis.get(i), mis.get(j));

                    outK.set(pair + "\t" + slot);
                    outV.set(Double.toString(contrib));
                    context.write(outK, outV);
                }
            }
        }
        private String stem(String w) {
            w = w.toLowerCase().trim();
            stemmer.setCurrent(w);
            stemmer.stem();
            return stemmer.getCurrent();
        }

        private String templateToP(String template) {
            if (template == null) return null;
            template = template.trim();
            if (template.isEmpty()) return null;

            String[] toks = template.split("\\s+");
            if (toks.length != 3 && toks.length != 4) return null;

            String leftVar = toks[0];
            String rightVar = toks[toks.length - 1];

            if (!(leftVar.equals("X") || leftVar.equals("Y"))) return null;
            if (!(rightVar.equals("X") || rightVar.equals("Y"))) return null;

            String verbStem = stem(toks[1]);

            // prep optional
            String prep = null;
            if (toks.length == 4) {
                prep = toks[2].toLowerCase().trim();
                if (!PREPS.contains(prep)) return null;
            }

            // === IMPORTANT NORMALIZATION ===
            // Your corpus paths are canonicalized as: X ... Y (X=nsubj, Y=obj/pobj).
            // So if the template is "Y ... X", flip it to "X ... Y".
            boolean flip = leftVar.equals("Y") && rightVar.equals("X");

            String outLeft = flip ? "X" : leftVar;
            String outRight = flip ? "Y" : rightVar;

            if (prep == null) {
                return outLeft + " " + verbStem + " " + outRight;
            } else {
                return outLeft + " " + verbStem + " " + prep + " " + outRight;
            }
        }
    }


    //SECTION: job4

    public static class IntersectionSumMapper extends Mapper<LongWritable, Text, Text, Text> {
        private final Text outK = new Text();
        private final Text outV = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) return;

            String[] parts = line.split("\t");
            if (parts.length != 4) return;
            outK.set(parts[0] + "\t" + parts[1] + "\t" + parts[2]);
            outV.set(parts[3]);
            context.write(outK, outV);
        }
    }

    public static class IntersectionSumReducer extends Reducer<Text, Text, Text, Text> {
        private final Text outK = new Text();
        private final Text outV = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            double sum = 0.0;
            for (Text val : values) {
                sum += Double.parseDouble(val.toString());
            }
            context.write(key, new Text(Double.toString(sum)));
        }
    }

//SECTION: job5

    public static class jobAIntersectionMapper extends Mapper<LongWritable, Text, Text, Text> {
        private final Text outK = new Text();
        private final Text outV = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) return;

            String[] parts = line.split("\t");
            if (parts.length != 4) return;

            String p1 = parts[0];
            String p2 = parts[1];
            String slot = parts[2];
            String intersection = parts[3];
            outK.set(p1 + "\t" + slot);
            outV.set("L" + "\t" +p2 +"\t" + intersection);
            context.write(outK, outV);
            outK.set(p2+ "\t" + slot);
            outV.set("R" + "\t" +p1 +"\t" + intersection);
            context.write(outK, outV);
        }
    }
    public static class jobAStrengthMapper extends Mapper<LongWritable, Text, Text, Text> {
        private final Text outK = new Text();
        private final Text outV = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) return;

            String[] parts = line.split("\t");
            if (parts.length != 3) return;

            String p = parts[0];
            String slot = parts[1];
            String svalue = parts[2];
            outK.set(p + "\t" + slot);
            outV.set("S" + "\t" +svalue );
            context.write(outK, outV);
        }
    }

    public static class jobAReducer extends Reducer<Text, Text, Text, Text> {
        private final Text outK = new Text();
        private final Text outV = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String[] keyParts = key.toString().split("\t", 2);
            if (keyParts.length != 2) return;
            String p = keyParts[0];
            String slot = keyParts[1];
            Double S = null;
            // store L/R records temporarily
            List<String[]> pairRecords = new ArrayList<>();

            for (Text v : values) {
                String[] parts = v.toString().split("\t");
                if (parts[0].equals("S")) {
                    // S(p,slot)
                    S = Double.parseDouble(parts[1]);
                } else {
                    // L or R record
                    if (parts.length == 3) pairRecords.add(parts);
                }
            }

            // if no strength, we cannot normalize later
            if (S == null) return;

            for (String[] rec : pairRecords) {
                String side = rec[0];       // L or R
                String otherP = rec[1];     // the other path
                String I = rec[2];          // intersection value

                String p1, p2;
                if (p.compareTo(otherP) <= 0) {
                    p1 = p;
                    p2 = otherP;
                } else {
                    p1 = otherP;
                    p2 = p;
                }

                outK.set(p1 + "\t" + p2 + "\t" + slot);
                outV.set(side + "\t" + I + "\t" + S);
                context.write(outK, outV);
            }
        }
    }

    public static class SimSlotMapper extends Mapper<LongWritable, Text, Text, Text> {
        private final Text outK = new Text();
        private final Text outV = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) return;

            String[] parts = line.split("\t");
            // expected: p1 p2 slot side I S
            if (parts.length != 6) return;

            String p1 = parts[0];
            String p2 = parts[1];
            String slot = parts[2];
            String side = parts[3];
            String I = parts[4];
            String S = parts[5];

            outK.set(p1 + "\t" + p2 + "\t" + slot);
            outV.set(side + "\t" + I + "\t" + S);
            context.write(outK, outV);
        }
    }


    public static class SimSlotReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            Double I = null;      // intersection sum
            Double S_left = null; // S(p1,slot)
            Double S_right = null;// S(p2,slot)

            for (Text v : values) {
                String[] parts = v.toString().split("\t");
                // expected: side \t I \t S
                if (parts.length != 3) continue;

                String side = parts[0];

                double iVal, sVal;
                try {
                    iVal = Double.parseDouble(parts[1]);
                    sVal = Double.parseDouble(parts[2]);
                } catch (Exception e) {
                    continue;
                }

                // I is the same in both L and R records, so just store it once
                if (I == null) I = iVal;

                if (side.equals("L")) S_left = sVal;
                else if (side.equals("R")) S_right = sVal;
            }

            if (I == null || S_left == null || S_right == null) return;

            double denom = S_left + S_right;
            if (denom <= 0) return;

            double simSlot = I / denom;

            // key is "p1\tp2\tslot"
            String[] kp = key.toString().split("\t", 3);
            if (kp.length != 3) return;

            String p1 = kp[0];
            String p2 = kp[1];
            String slot = kp[2];

            context.write(new Text(p1 + "\t" + p2), new Text(slot + "\t" + Double.toString(simSlot)));
        }
    }

    public static class finalCalculationMapper extends Mapper<LongWritable, Text, Text, Text> {
        private final Text outK = new Text();
        private final Text outV = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) return;

            String[] parts = line.split("\t");
            // expected: p1 p2 slot side I S
            if (parts.length != 4) return;

            String p1 = parts[0];
            String p2 = parts[1];
            String slot = parts[2];
            String simSlot = parts[3];

            outK.set(p1 + "\t" + p2);
            outV.set(slot + "\t" + simSlot);
            context.write(outK, outV);
        }
    }


    public static class finalCalculationReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            Double simX = null;
            Double simY = null;

            for (Text v : values) {
                String[] parts = v.toString().split("\t");
                if (parts.length != 2) continue;

                String slot = parts[0];
                double sim;
                try {
                    sim = Double.parseDouble(parts[1]);
                } catch (Exception e) {
                    continue;
                }

                if (slot.equals("X")) {
                    simX = sim;
                } else if (slot.equals("Y")) {
                    simY = sim;
                }
            }

            // Must have both X and Y
            if (simX == null || simY == null) return;

            double finalSim = Math.sqrt(simX * simY);

            context.write(key, new Text(Double.toString(finalSim)));
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println(
                    "Usage: SimilarityComputation <job5B_input> <job6_output>"
            );
            System.exit(-1);
        }

        System.setProperty("hadoop.home.dir",
                "C:/src/hadoop/hadoop-3.3.5/winutils-master/hadoop-3.3.5");

        Configuration conf = new Configuration();

        // LOCAL testing only
        conf.set("mapreduce.framework.name", "local");
        conf.set("fs.defaultFS", "file:///");

        Job job = Job.getInstance(conf, "Job6 - Final path similarity (Formula 3)");
        job.setJarByClass(SimilarityComputation.class);

        // Mapper groups by (p1,p2)
        job.setMapperClass(finalCalculationMapper.class);

        // Final reducer
        job.setReducerClass(finalCalculationReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));   // Job5B output
        FileOutputFormat.setOutputPath(job, new Path(args[1])); // Job6 output

        boolean ok = job.waitForCompletion(true);
        if (!ok) throw new RuntimeException("Job6 failed");
    }

}
