package com.richard.lhkpi;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

import com.richard.lhfile.LHHDFSFileManger;
import com.richard.lhutils.lhutils;

public class KPIRegion
{
    public static class KPIRegionMapper extends MapReduceBase implements Mapper<Object, Text, Text, IntWritable> {
        private IntWritable one = new IntWritable(1);
        private Text word = new Text();

        @Override
        public void map(Object key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
            LHAccessRecord kpi = LHAccessRecord.getInstance().filterBroswer(value.toString());
            if (kpi.isValid())
            {  
            	LHAccessRecord.g_processCount++;
            	String region = lhutils.ipRegion(kpi.getRemote_addr()); 
                word.set(region);
                output.collect(word, one);
            }
        }
    }

    public static class KPIRegionReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, Text> {
        private Text result = new Text();

        @Override
        public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            int sum = 0;
            while (values.hasNext())
            {
                sum += values.next().get();
            }
            
            result.set(String.valueOf(sum)+ " " + "所占百分比" + " " + lhutils.getPercent(sum, LHAccessRecord.g_processCount));
            output.collect(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        String input = "hdfs://192.168.1.4:9000/lihui/accesslog/";
        String output = "hdfs://192.168.1.4:9000/lihui/kpiregion/";

        JobConf conf = new JobConf(KPIRegion.class);
        
        //如果输出文件夹存在，则删除，否则会报错
        if (LHHDFSFileManger.fileExists(output, conf))
        {
            LHHDFSFileManger.deleteFile(output, conf);        	
        }
        
        conf.setJobName("KPIRegion");
        
        conf.setMapOutputKeyClass(Text.class);
        conf.setMapOutputValueClass(IntWritable.class);
        
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

        conf.setMapperClass(KPIRegionMapper.class);
        conf.setReducerClass(KPIRegionReducer.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(input));
        FileOutputFormat.setOutputPath(conf, new Path(output));

        JobClient.runJob(conf);
                
        System.exit(0);
    }

}
