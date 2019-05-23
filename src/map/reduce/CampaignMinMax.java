package map.reduce;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CampaignMinMax {

    public static class DurationMapper
            extends Mapper<Object, Text, Text, MinMaxDuration>{

        private Text month= new Text();
        private Integer minduration;
        private Integer maxduration;


        private MinMaxDuration outPut= new MinMaxDuration();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String[] campaignFields= value.toString().split(",");
            //10001,'telephone','may','mon',100,1,999,0,'nonexistent','no',0
            month.set(campaignFields[2]);
            minduration=Integer.parseInt(campaignFields[4]);
            maxduration=Integer.parseInt(campaignFields[4]);

            if (month == null || minduration == null || maxduration== null) {
                return;
            }

            try {
                outPut.setMinDuration(minduration);
                outPut.setMaxDuration(maxduration);
                context.write(month,outPut);

            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }

    public static class DurationReducer extends Reducer<Text,MinMaxDuration,Text,MinMaxDuration> {

        private MinMaxDuration resultRow = new MinMaxDuration();

        public void reduce(Text key, Iterable<MinMaxDuration> values, Context context) throws IOException, InterruptedException {

            Integer minduration = 0;
            Integer maxduration = 0;

            resultRow.setMinDuration(null);
            resultRow.setMaxDuration(null);

            for (MinMaxDuration val : values) {

                minduration = val.getMinDuration();
                maxduration = val.getMaxDuration();

                // get min score
                if (resultRow.getMinDuration()==null || minduration.compareTo(resultRow.getMinDuration())<0) {
                    resultRow.setMinDuration(minduration);
                }

                // get min bonus
                if (resultRow.getMaxDuration()==null || maxduration.compareTo(resultRow.getMaxDuration())>0) {
                    resultRow.setMaxDuration(maxduration);
                }
        } // end of for loop
     context.write(key, resultRow);
    }
}

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "Campaign Duration Count");

        job.setJarByClass(CampaignMinMax.class);
        job.setMapperClass(DurationMapper.class);
        job.setReducerClass(DurationReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(MinMaxDuration.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(MinMaxDuration.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}