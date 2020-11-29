import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;

public class CountDegree extends Configured implements Tool {
    public static class FirstMapper extends Mapper<LongWritable, Text, LongWritable, LongWritable> {
        public void map(LongWritable key, Text text, Context context) throws IOException, InterruptedException {
            // a b --> a b, b a
            String[] pair = text.toString().split("\\s+");
            if (pair.length > 1) {
                Long u = Long.parseLong(pair[0]);
                Long v = Long.parseLong(pair[1]);

                context.write(new LongWritable(u), new LongWritable(v));
                context.write(new LongWritable(v), new LongWritable(u));
            }
        }
    }

    public static class FirstReducer extends Reducer<LongWritable, LongWritable, LongWritable, Text> {
        public void reduce(LongWritable key, Iterable<LongWritable> values, Context context)
                throws IOException, InterruptedException {
            // hapus duplikat & u v --> u v du
            LinkedHashSet<Long> valuesCopy = new LinkedHashSet<Long>();
            for (LongWritable u : values) {
                valuesCopy.add((long) u.get());
            }
            for (Long u : valuesCopy) {
                context.write(key, new Text(u.toString() + " " + valuesCopy.size()));
            }
        }
    }

    public static class SecondMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
        public void map(LongWritable key, Text text, Context context) throws IOException, InterruptedException {
            // u v du --> v u du
            String[] pair = text.toString().split("\\s+");
            if (pair.length > 2) {
                Long u = Long.parseLong(pair[0]);
                Long v = Long.parseLong(pair[1]);
                Long du = Long.parseLong(pair[2]);

                context.write(new LongWritable(v), new Text(u.toString() + " " + du));
            }
        }
    }

    public static class SecondReducer extends Reducer<LongWritable, Text, LongWritable, Text> {
        public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // v u du --> u v du dv
            Long v = key.get();
            ArrayList<Text> valuesCopy = new ArrayList<Text>();
            for (Text text : values) {
                valuesCopy.add(text);
            }
            for (Text text : valuesCopy) {                
                String[] pair = text.toString().split("\\s+");
                Long u = Long.parseLong(pair[0]);
                Long du = Long.parseLong(pair[1]);

                context.write(new LongWritable(u), new Text("" + v + " " + du + " " + valuesCopy.size()));
            }
        }
    }

    public int run(String[] args) throws Exception {
        Job jobOne = new Job(getConf());
        jobOne.setJobName("first-degree");

        jobOne.setMapOutputKeyClass(LongWritable.class);
        jobOne.setMapOutputValueClass(LongWritable.class);

        jobOne.setOutputKeyClass(LongWritable.class);
        jobOne.setOutputValueClass(Text.class);

        jobOne.setJarByClass(CountDegree.class);
        jobOne.setMapperClass(FirstMapper.class);
        jobOne.setReducerClass(FirstReducer.class);

        FileInputFormat.addInputPath(jobOne, new Path(args[0]));
        FileOutputFormat.setOutputPath(jobOne, new Path("/user/hadoop/temp/first-preprocessing"));

        Job jobTwo = new Job(getConf());
        jobTwo.setJobName("second-mapreduce");

        jobTwo.setMapOutputKeyClass(LongWritable.class);
        jobTwo.setMapOutputValueClass(Text.class);

        jobTwo.setOutputKeyClass(LongWritable.class);
        jobTwo.setOutputValueClass(Text.class);

        jobTwo.setJarByClass(CountDegree.class);
        jobTwo.setMapperClass(SecondMapper.class);
        jobTwo.setReducerClass(SecondReducer.class);

        FileInputFormat.addInputPath(jobTwo, new Path("/user/hadoop/temp/first-preprocessing"));
        FileOutputFormat.setOutputPath(jobTwo, new Path(args[1]));

        int ret = jobOne.waitForCompletion(true) ? 0 : 1;
        if (ret == 0)
            ret = jobTwo.waitForCompletion(true) ? 0 : 1;

        return ret;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new CountDegree(), args);
        System.exit(res);
    }
}
