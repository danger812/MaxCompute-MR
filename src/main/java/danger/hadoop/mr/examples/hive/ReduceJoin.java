package danger.hadoop.mr.examples.hive;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Mapper;


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.Path;


public class ReduceJoin extends Configured implements Tool {
    public static class JoinMapper extends
            Mapper<LongWritable, Text, Text, Text> {
        String employeeValue = "";

        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            /*
             * 根据命令行传入的文件名，判断数据来自哪个文件，来自employee的数据打上a标签，来自dept的数据打上b标签
             */
            String filepath = ((FileSplit) context.getInputSplit()).getPath().toString();
            String line = value.toString();
            if (line == null || line.equals("")) return;

            if (filepath.indexOf("employee") != -1) {
                String[] lines = line.split(" ");
                if (lines.length < 4) return;

                String deptNo = lines[3];
                employeeValue = line + " a";
                context.write(new Text(deptNo), new Text(employeeValue));
            } else if (filepath.indexOf("dept") != -1) {
                String[] lines = line.split(" ");
                if (lines.length < 2) return;
                String deptNo = lines[0];
                context.write(new Text(deptNo), new Text(line +" b"));
            }
        }
    }

    public static class JoinReducer extends
            Reducer<Text, Text, Text, NullWritable> {
        protected void reduce(Text key, Iterable<Text> values,
                              Context context) throws IOException, InterruptedException {
            List<String[]> lista = new ArrayList<String[]>();
            List<String[]> listb = new ArrayList<String[]>();

            for (Text val : values) {
                String[] str = val.toString().split(" ");
                //最后一位是标签位，因此根据最后一位判断数据来自哪个文件，标签为a的数据放在lista中，标签为b的数据放在listb中
                String flag = str[str.length - 1];
                if ("a".equals(flag)) {
                    //String valueA = str[0]  " "  str[1]  " "  str[2];
                    lista.add(str);
                } else if ("b".equals(flag)) {
                    //String valueB = str[0]  " "  str[1];
                    listb.add(str);
                }
            }

            for (int i = 0; i < lista.size(); i++) {
                if (listb.size() == 0) {
                    continue;
                } else {
                    String[] stra = lista.get(i);
                    for (int j = 0; j < listb.size(); j++) {
                        String[] strb = listb.get(j);
                        String keyValue = stra[0] + " " + stra[1] + " " + stra[2] + " " +stra[3] +" "+ strb[1];
                        context.write(new Text(keyValue), NullWritable.get());
                    }
                }
            }
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        GenericOptionsParser optionparser = new GenericOptionsParser(conf, args);
        conf = optionparser.getConfiguration();
        Job job = Job.getInstance(conf, "Reduce side join");
        job.setJarByClass(ReduceJoin.class);
        //1.1 设置输入目录和设置输入数据格式化的类
        //FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileInputFormat.addInputPaths(job, conf.get("input_data"));

        job.setInputFormatClass(TextInputFormat.class);

        //1.2 设置自定义Mapper类和设置map函数输出数据的key和value的类型
        job.setMapperClass(JoinMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        //1.3 设置reduce数量
        job.setNumReduceTasks(1);
        //设置实现了reduce函数的类
        job.setReducerClass(JoinReducer.class);

        //设置reduce函数的key值
        job.setOutputKeyClass(Text.class);
        //设置reduce函数的value值
        job.setOutputValueClass(NullWritable.class);

        // 判断输出路径是否存在，如果存在，则删除
        Path output_dir = new Path(conf.get("output_dir"));
        FileSystem hdfs = output_dir.getFileSystem(conf);
        if (hdfs.isDirectory(output_dir)) {
            hdfs.delete(output_dir, true);
        }

        FileOutputFormat.setOutputPath(job, output_dir);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new ReduceJoin(), args);
        System.exit(exitCode);
    }
}
