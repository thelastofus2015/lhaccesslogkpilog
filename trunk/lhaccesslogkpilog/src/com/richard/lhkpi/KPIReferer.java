package com.richard.lhkpi;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.fs.Path;
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

public class KPIReferer
{
    public static class KPIRefererMapper extends MapReduceBase implements Mapper<Object, Text, Text, Text> 
    {
        private Text word = new Text();
        private Text ips = new Text();

        @Override
        public void map(Object key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            LHAccessRecord kpi = LHAccessRecord.getInstance().filterDomain(value.toString());
            if (kpi.isValid())
            {                
                word.set(kpi.getHttp_referer());
                ips.set(kpi.getRemote_addr());
                output.collect(word, ips);
            }
        }
    }

    public static class KPIRefererReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text>
    {
        private Text result = new Text();
        private Set<String> count = new HashSet<String>();

        @Override
        public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            while (values.hasNext()) {
                count.add(values.next().toString());
            }
            result.set("来源网站带来Ip数" + " " + String.valueOf(count.size()));
            output.collect(key, result);
        }
    }

    public static void main(String[] args) throws Exception
    {
        String input = "hdfs://192.168.1.4:9000/lihui/accesslog/";
        String output = "hdfs://192.168.1.4:9000/lihui/kpiReferer/";
        

        JobConf conf = new JobConf(KPIReferer.class);
        
        //如果输出文件夹存在，则删除，否则会报错
        if (LHHDFSFileManger.fileExists(output, conf))
        {
            LHHDFSFileManger.deleteFile(output, conf);        	
        }
        
        conf.setJobName("KPIReferer");
        
        conf.setMapOutputKeyClass(Text.class);
        conf.setMapOutputValueClass(Text.class);
        
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);
        
        conf.setMapperClass(KPIRefererMapper.class);
        conf.setReducerClass(KPIRefererReducer.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(input));
        FileOutputFormat.setOutputPath(conf, new Path(output));

        JobClient.runJob(conf);
        System.exit(0);
    }
}
