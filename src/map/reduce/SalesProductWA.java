package map.reduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 3.1. totalVendasAno - Qual o total de vendas por país durante os 3 anos?
 */
public class SalesProductWA {

    public class WASalesData {

        public static final int retailer_country_ind = 0;
        public static final int order_method_type_ind = 1;
        public static final int retailer_type_ind = 2;
        public static final int product_line_ind = 3;
        public static final int product_type_ind = 4;
        public static final int product_ind = 5;
        public static final int year_ind = 6;
        public static final int quarter_ind = 7;
        public static final int revenue_ind = 8;
        public static final int quantity_ind = 9;
        public static final int gross_margin_ind = 10;


        //      Retailer country,Order method type,Retailer type,Product line,Product type,Product,Year,Quarter,Revenue,Quantity,Gross margin
        //      United States,Fax,Outdoors Shop,Camping Equipment,Cooking Gear,TrailChef Deluxe Cook Set,2012,Q1 2012,59628.66,489,0.34754797

    }

    public static class SalesTotalMapper
            extends Mapper<Object, Text, Text, FloatWritable> {

        @Override
        public void map(Object key, Text value, Mapper<Object, Text, Text, FloatWritable>.Context context)
                throws IOException, InterruptedException {

            String[] parts = value.toString().split(",");

            try {
                context.write(new Text(parts[WASalesData.retailer_country_ind]), new FloatWritable(Float.parseFloat(parts[WASalesData.revenue_ind])));

            } catch (Exception e) {
                e.printStackTrace();
            }

        }
    }

    public static class SalesTotaReducer
            extends Reducer<Text, FloatWritable, Text, FloatWritable> {

        @Override
        public void reduce(Text key, Iterable<FloatWritable> values, Context context)
                throws IOException, InterruptedException {

            float sum = 0;

            for (FloatWritable val : values) {
                sum += val.get();
            }

            context.write(key, new FloatWritable(sum));
        }
    }

    public static void main(String[] args)
            throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();

        Job jobSaleTotal =  Job.getInstance(conf,"sale total");
        jobSaleTotal.setJarByClass(SalesProductWA.class);
        jobSaleTotal.setMapperClass(SalesTotalMapper.class);
        jobSaleTotal.setCombinerClass(SalesTotaReducer.class);
        jobSaleTotal.setReducerClass(SalesTotaReducer.class);
        jobSaleTotal.setOutputKeyClass(Text.class);
        jobSaleTotal.setOutputValueClass(FloatWritable.class);
        FileInputFormat.addInputPath(jobSaleTotal, new Path(args[0]));
        FileOutputFormat.setOutputPath(jobSaleTotal, new Path(args[1]));
        System.exit(jobSaleTotal.waitForCompletion(true) ? 0 : 1);
    }
}

