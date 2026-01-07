package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.tartarus.snowball.ext.EnglishStemmer;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;


public class DirtExtraction {

    public static class DirtMapper extends Mapper<LongWritable, Text, Text, Text> {
        private final EnglishStemmer stemmer = new EnglishStemmer();
        private final Text outKey = new Text();
        private final Text outValue = new Text();

        private final Set<String> auxiliaryVerbs = new HashSet<>(Arrays.asList(
                "be", "is", "am", "are", "was", "were", "been", "being",
                "have", "has", "had", "do", "does", "did"
        ));

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // הקלט הוא שורה מפורמט ה-Biarcs של גוגל [cite: 29, 30]
            String line = value.toString();
            String[] tokens = line.split("\t");

            /* לוגיקת החילוץ (סכימה כללית):
               1. עליך לפרק את ה-tokens כדי למצוא את ה-Head (חייב להיות Verb)[cite: 18].
               2. עליך למצוא את ה-Slots X ו-Y (חייבים להיות Nouns)[cite: 17].
               3. עליך לכלול מילות יחס (IN/TO) בנתיב.
            */

            // דוגמה לחילוץ מילה וביצוע סטמינג [cite: 24]
            String rawWord = "involved"; // נניח שזו המילה שחולצה מהסלוט
            String stemmedWord = applyStemming(rawWord);

            if (stemmedWord != null) {
                // המפתח: נתיב וסלוט. הערך: המילה המעובדת וספירה 1.
                outKey.set("path_example:X");
                outValue.set(stemmedWord + ":1");
                context.write(outKey, outValue);
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
    }

    // --- 2. ה-REDUCER: סכימת מופעים לחישוב MI בהמשך ---
    public static class DirtReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // בשלב זה נסכום כמה פעמים מילה הופיעה בנתיב-סלוט מסוים
            // זהו הבסיס לחישוב ה-mi(p, Slot, w) הנדרש [cite: 36]
            long sum = 0;
            String word = "";

            for (Text val : values) {
                String[] parts = val.toString().split(":");
                word = parts[0];
                sum += Long.parseLong(parts[1]);
            }

            context.write(key, new Text(word + "\t" + sum));
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: DirtExtraction <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "DIRT Extraction Job");

        job.setJarByClass(DirtExtraction.class);
        job.setMapperClass(DirtMapper.class);
        job.setReducerClass(DirtReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}

























