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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

public class MiComputation {

    // Mapper: reads Job1 output lines: "<field0>\t<field1>\t<field2>\t<count>"
    // Skips globals (* and TOTAL) and groups by (p,slot)
    public static class MiMapper extends Mapper<LongWritable, Text, Text, Text> {
        private final Text outK = new Text();
        private final Text outV = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) return;

            String[] parts = line.split("\t");
            if (parts.length != 4) return;

            String p = parts[0];
            if (p.equals("*") || p.equals("TOTAL")) return; // globals -> not sent to reducer

            String slot = parts[1];
            String w = parts[2];
            String cnt = parts[3];

            outK.set(p + "\t" + slot);      // group by (p,slot)
            outV.set(w + "\t" + cnt);       // (wordOrStar, count)
            context.write(outK, outV);
        }
    }

    public static class MiReducer extends Reducer<Text, Text, Text, Text> {

        // c(*,slot,w)  key: slot+"\t"+w
        private final Map<String, Long> starCounts = new HashMap<>();
        // N_slot       key: slot
        private final Map<String, Long> totalSlot = new HashMap<>();

        @Override
        protected void setup(Context context) throws IOException {
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles == null) return;

            Configuration conf = context.getConfiguration();
            FileSystem fs = FileSystem.get(conf);

            for (URI uri : cacheFiles) {
                Path path = new Path(uri.toString());
                try (FSDataInputStream in = fs.open(path);
                     BufferedReader br = new BufferedReader(new InputStreamReader(in))) {

                    String line;
                    while ((line = br.readLine()) != null) {
                        line = line.trim();
                        if (line.isEmpty()) continue;

                        String[] parts = line.split("\t");
                        // globals.txt should contain ONLY:
                        // "*\tSLOT\tWORD\tCOUNT"
                        // "TOTAL\tSLOT\tCOUNT"
                        if (parts.length == 4 && parts[0].equals("*")) {
                            String slot = parts[1];
                            String w = parts[2];
                            long cnt = parseLongSafe(parts[3]);
                            starCounts.put(slot + "\t" + w, cnt);
                        } else if (parts.length == 3 && parts[0].equals("TOTAL")) {
                            String slot = parts[1];
                            long cnt = parseLongSafe(parts[2]);
                            totalSlot.put(slot, cnt);
                        }
                    }
                }
            }
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            // key = "p\tSLOT"
            String[] ks = key.toString().split("\t");
            if (ks.length != 2) return;
            String p = ks[0];
            String slot = ks[1];

            long c_ps_all = -1; // c(p,slot,*)
            Map<String, Long> c_psw = new HashMap<>(); // word -> c(p,slot,word)

            for (Text v : values) {
                String[] pv = v.toString().split("\t");
                if (pv.length != 2) continue;

                String w = pv[0];
                long cnt = parseLongSafe(pv[1]);

                if (w.equals("*")) c_ps_all = cnt;
                else c_psw.put(w, c_psw.getOrDefault(w, 0L) + cnt);
            }

            if (c_ps_all <= 0) return;

            Long NsObj = totalSlot.get(slot);
            if (NsObj == null || NsObj <= 0) return;
            long N_s = NsObj;

            for (Map.Entry<String, Long> e : c_psw.entrySet()) {
                String w = e.getKey();
                long c_psw_cnt = e.getValue();
                if (c_psw_cnt <= 0) continue;

                Long c_star = starCounts.get(slot + "\t" + w);
                if (c_star == null || c_star <= 0) continue;

                double mi = Math.log(((double) c_psw_cnt * (double) N_s) /
                        ((double) c_ps_all * (double) c_star));

                context.write(new Text(p + "\t" + slot + "\t" + w), new Text(Double.toString(mi)));
            }
        }

        private static long parseLongSafe(String s) {
            try { return Long.parseLong(s.trim()); }
            catch (Exception e) { return 0L; }
        }
    }

    //SECTION: S(p)
    public static class PathStrengthMapper extends Mapper<LongWritable, Text, Text, Text> {
        private final Text outK = new Text();
        private final Text outV = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) return;

            String[] parts = line.split("\t");
            if (parts.length != 4) return;
            String pslot = parts[0] + "\t" + parts[1];
            String mi = parts[3];
            outK.set(pslot);
            outV.set(mi);
            context.write(outK, outV);
        }
    }

    public static class PathStrengthReducer extends Reducer<Text, Text, Text, Text> {
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

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: PathStrength <job2_mi_input> <pathStrength_output>");
            System.exit(-1);
        }

        System.setProperty("hadoop.home.dir",
                "C:/src/hadoop/hadoop-3.3.5/winutils-master/hadoop-3.3.5");

        Configuration conf = new Configuration();

        // LOCAL testing only
        conf.set("mapreduce.framework.name", "local");
        conf.set("fs.defaultFS", "file:///");

        Job job = Job.getInstance(conf, "PathStrength - Sum MI per (p,slot)");
        job.setJarByClass(MiComputation.class); // or your main class

        job.setMapperClass(PathStrengthMapper.class);
        job.setReducerClass(PathStrengthReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));   // Job2 output
        FileOutputFormat.setOutputPath(job, new Path(args[1])); // PathStrength output

        boolean ok = job.waitForCompletion(true);
        if (!ok) throw new RuntimeException("PathStrength job failed");
    }
}
