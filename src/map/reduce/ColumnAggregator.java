package map.reduce;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * @author Unmesha SreeVeni U.B
 */
public class ColumnAggregator {

    public static class ColMapper extends
            Mapper<Object, Text, Text, FloatWritable> {
        /*
         * Emits column Id as key and entire column elements as Values
         */
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String[] cols = value.toString().split(",");

            for (int i = 0; i < cols.length; i++) {

                context.write(new Text(String.valueOf(i + 1)), new FloatWritable(Float.parseFloat(cols[i])));
            }

        }
    }

    public static class ColReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {
        /*
         * Reducer finds min and max of each column
         */

        public void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {

            float min = Integer.MAX_VALUE, max = 0;

            Iterator<FloatWritable> iterator = values.iterator(); //Iterating

            while (iterator.hasNext()) {

                float value = iterator.next().get();

                if (value < min) { //Finding min value
                    min = value;
                }

                if (value > max) { //Finding max value
                    max = value;
                }
            }
            context.write(new Text(key), new FloatWritable(min));
            context.write(new Text(key), new FloatWritable(max));
        }
    }
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        Job job = new Job(conf, "Min and Max");
        job.setJarByClass(ColumnAggregator.class);
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(new Path(args[1]))) {
            fs.delete(new Path(args[1]), true);
        }
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FloatWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);

        job.setMapperClass(ColMapper.class);
        job.setReducerClass(ColReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}