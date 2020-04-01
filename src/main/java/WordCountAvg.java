import com.google.common.base.Charsets;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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
import java.util.StringTokenizer;

public class WordCountAvg {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        //delete if output path is available
        Path outputPath = new Path(args[1]);
        FileSystem fs = FileSystem.get(new URI(outputPath.toString()), conf);
        fs.delete(outputPath, true);

        job.setJarByClass(WordCountAvg.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        // to run locally
        //FileInputFormat.addInputPath(job, new Path("/home/hashan/Code/mapreduce/wordcountavg/src/main/resources/wordfile.txt"));
        //FileOutputFormat.setOutputPath(job, new Path("/home/hashan/Code/mapreduce/wordcountavg/src/main/resources/output"));
        int result = job.waitForCompletion(true) ? 0 : 1;
        System.out.println("Job end");

        // avg cal
        Path file = new Path(outputPath.toString(), "part-r-00000");
        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(file), Charsets.UTF_8));
        int count = 0;
        double total = 0;
        String line = br.readLine();
        while (line != null) {
            count += 1;
            String s = line.split("\t")[1].trim();
            if (StringUtils.isNumeric(s)) {
                total += Double.parseDouble(s);
            }
            line = br.readLine();
        }
        double avg = total / count;
        System.out.println("Average word count is " + avg);
        System.exit(result);
    }

    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {


        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(word, one);
            }
        }
    }

    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }
}