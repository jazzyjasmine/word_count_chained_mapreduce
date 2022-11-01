import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class WordCount {
    public static class WordToCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] words = line.split("\\s+");
            for (String word : words) {
                Text outputKey = new Text(word.trim());
                IntWritable outputValue = new IntWritable(1);
                context.write(outputKey, outputValue);
            }
        }
    }

    public static class CountToWordsMapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String [] keyValuePair = value.toString().split("\\s+");
            context.write(new Text(keyValuePair[1]), new Text(keyValuePair[0]));
        }
    }

    public static class WordToCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int countSum = 0;
            for (IntWritable count : values) {
                countSum += count.get();
            }
            context.write(key, new IntWritable(countSum));
        }
    }

    public static class CountToWordsReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            List<String> words = new ArrayList<>();
            for (Text word: values) {
                words.add(word.toString());
            }
            context.write(key, new Text(String.join(", ", words)));
        }
    }

    public static void main(String[] args) throws Exception {
        // Reference: https://medium.com/edureka/mapreduce-tutorial-3d9535ddbe7c
        // 1-round MapReduce: Get count for each word, e.g. "the    2"
        Configuration wordToCountConfig = new Configuration();
        Job wordToCountJob = new Job(wordToCountConfig, "Word-to-count");

        wordToCountJob.setJarByClass(WordCount.class);
        wordToCountJob.setMapperClass(WordToCountMapper.class);
        wordToCountJob.setReducerClass(WordToCountReducer.class);

        wordToCountJob.setOutputKeyClass(Text.class);
        wordToCountJob.setOutputValueClass(IntWritable.class);

        Path inputPath = new Path(args[0]);
        Path outputPathTemp = new Path(args[1]);
        FileInputFormat.addInputPath(wordToCountJob, inputPath);
        FileOutputFormat.setOutputPath(wordToCountJob, outputPathTemp);

        wordToCountJob.waitForCompletion(true);

        // 2-round MapReduce: Get a list of words for each count, e.g. "2   fox, the"
        Configuration countToWordsConfig = new Configuration();
        Job countToWordsJob = new Job(countToWordsConfig, "Count-to-words");

        countToWordsJob.setJarByClass(WordCount.class);
        countToWordsJob.setMapperClass(CountToWordsMapper.class);
        countToWordsJob.setReducerClass(CountToWordsReducer.class);

        countToWordsJob.setOutputKeyClass(Text.class);
        countToWordsJob.setOutputValueClass(Text.class);

        Path outputPathFinal = new Path(args[2]);
        FileInputFormat.addInputPath(countToWordsJob, outputPathTemp);
        FileOutputFormat.setOutputPath(countToWordsJob, outputPathFinal);

        System.exit(countToWordsJob.waitForCompletion(true) ? 0 : 1);
    }
}
