package map.reduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 2.4. precoMedioProdutoPais – Qual o preço médio de cada produto, em cada país? Apresente os resultados
 * ordenados por produto
 */
public class SalesAvgPriceProductWA {

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
    /** 2.4. precoMedioProdutoPais – Qual o preço médio de cada produto, em cada país? Apresente os resultados ordenados por produto */

    public static class SalesCountryAvgProductMapper
            extends Mapper<Object, Text, Text, Text> {

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] parts = value.toString().split(",");

                context.write(new Text(parts[WASalesData.retailer_country_ind] + ", " + parts[WASalesData.product_ind]+","), new Text(parts[WASalesData.revenue_ind]
                              + "," + parts[WASalesData.quantity_ind]));
        }
    }

    /** 2.4. precoMedioProdutoPais – Qual o preço médio de cada produto, em cada país? Apresente os resultados ordenados por produto */

    public static class SalesCountryAvgProductReducer
            extends Reducer<Text, Text, Text, FloatWritable> {

            @Override
            public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

                float sumRevenue = 0;
                float countQty = 0;

                for (Text val : values) {

                    if (val != null){

                        String[] parts = val.toString().split(",");

                        try {
                            float revenue = Float.parseFloat(parts[0]);
                            float qty = Float.parseFloat(parts[1]);

                            sumRevenue += revenue;
                            countQty += qty;

                        }catch (NumberFormatException e) {}
                    }
                }

                context.write(key, new FloatWritable(sumRevenue / countQty));
            }
    }

    public static void main(String[] args)
            throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();

        Job jobSaleTotal =  Job.getInstance(conf,"product average");

        jobSaleTotal.setJarByClass(SalesAvgPriceProductWA.class);
        jobSaleTotal.setMapperClass(SalesCountryAvgProductMapper.class);
        jobSaleTotal.setReducerClass(SalesCountryAvgProductReducer.class);
        jobSaleTotal.setMapOutputKeyClass(Text.class);
        jobSaleTotal.setMapOutputValueClass(Text.class);
        jobSaleTotal.setOutputKeyClass(Text.class);
        jobSaleTotal.setOutputValueClass(FloatWritable.class);
        FileInputFormat.addInputPath(jobSaleTotal, new Path(args[0]));
        FileOutputFormat.setOutputPath(jobSaleTotal, new Path(args[1]));
        System.exit(jobSaleTotal.waitForCompletion(true) ? 0 : 1);
    }
}

