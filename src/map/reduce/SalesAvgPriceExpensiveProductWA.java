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
 * 2.5. produtoPaisMaisCaro – Para cada produto, qual o país que tem o preço médio de venda mais elevado? E
 * qual é esse preço? Utilize como input o output da função precoMedioProdutoPais
 */
public class SalesAvgPriceExpensiveProductWA {

    public class WASalesData {

        public static final int retailer_country = 0;
        public static final int product = 1;
        public static final int avg_price = 2;


        //      Retailer country,Order method type,Retailer type,Product line,Product type,Product,Year,Quarter,Revenue,Quantity,Gross margin
        //      United States,Fax,Outdoors Shop,Camping Equipment,Cooking Gear,TrailChef Deluxe Cook Set,2012,Q1 2012,59628.66,489,0.34754797

    }
    /** 2.5. produtoPaisMaisCaro – Para cada produto, qual o país que tem o preço médio de venda mais elevado? E
     * qual é esse preço? Utilize como input o output da função precoMedioProdutoPais */

    public static class SalesCountryAvgExpensiveProductMapper
            extends Mapper<Object, Text, Text, Text> {

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] parts = value.toString().split(",");

            try {
                context.write(new Text(parts[SalesAvgPriceExpensiveProductWA.WASalesData.product]+","), new Text(parts[SalesAvgPriceExpensiveProductWA.WASalesData.retailer_country]
                        + "," + parts[SalesAvgPriceExpensiveProductWA.WASalesData.avg_price]));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    /** 2.5. produtoPaisMaisCaro – Para cada produto, qual o país que tem o preço médio de venda mais elevado? E
             qual é esse preço? Utilize como input o output da função precoMedioProdutoPais */

    public static class SalesCountryAvgExpensiveProductReducer
            extends Reducer<Text, Text, Text, Text> {

            @Override
            public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

                float max = 0;
                String pais = "";

                for (Text val : values) {

                    if (val != null){

                        String[] parts = val.toString().split(",");

                        try {
                            float value = Float.parseFloat(parts[1]);

                            if (value > max) { //Finding max value
                                max = value;
                                pais = parts[0];
                            }
                        } catch (NumberFormatException e) {}
                    }
                }
                context.write(key, new Text(pais+","+max));
            }
    }

    public static void main(String[] args)
            throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();

        Job jobSaleTotal =  Job.getInstance(conf,"product average");

        jobSaleTotal.setJarByClass(SalesAvgPriceExpensiveProductWA.class);

        jobSaleTotal.setMapperClass(SalesCountryAvgExpensiveProductMapper.class);
        jobSaleTotal.setReducerClass(SalesCountryAvgExpensiveProductReducer.class);

        jobSaleTotal.setMapOutputKeyClass(Text.class);
        jobSaleTotal.setMapOutputValueClass(Text.class);

        jobSaleTotal.setOutputKeyClass(Text.class);
        jobSaleTotal.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(jobSaleTotal, new Path(args[0]));
        FileOutputFormat.setOutputPath(jobSaleTotal, new Path(args[1]));

        System.exit(jobSaleTotal.waitForCompletion(true) ? 0 : 1);
    }
}

