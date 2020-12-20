package danger.hadoop.mr.examples.hive;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.Path;

public class MapJoin extends Configured implements Tool {

    public static class MapJoinMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
        private Map<Integer, String> deptData = new HashMap<Integer, String>();

        @Override
        protected void setup(Mapper<LongWritable, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {
            super.setup(context);
            //从缓存的中读取文件。
            Path[] files = context.getLocalCacheFiles();
            //            Path file1path = new Path(files[0]);
            BufferedReader reader = new BufferedReader(new FileReader(files[0].toString()));
            String str = null;
            try {
                // 一行一行读取
                while ((str = reader.readLine()) != null) {
                    // 对缓存中的数据以" "分隔符进行分隔。
                    String[] splits = str.split(" ");
                    // 把需要的数据放在Map中。注意不能操作Map的大小，否则会出现OOM的异常
                    deptData.put(Integer.parseInt(splits[0]), splits[1]);
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                reader.close();
            }
        }

        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context) throws IOException,
                InterruptedException {
            // 获取从HDFS中加载的表
            String[] values = value.toString().split(" ");
            // 获取关联字段depNo，这个字段是关键
            int depNo = Integer.parseInt(values[3]);
            // 根据deptNo从内存中的关联表中获取要关联的属性depName
            String depName = deptData.get(depNo);
            String resultData = value.toString() + " " + depName;
            // 将数据通过context写入到Reduce中。
            context.write(new Text(resultData), NullWritable.get());
        }
    }

    public static class MapJoinReducer extends Reducer<Text, NullWritable, Text, NullWritable> {
        public void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            context.write(key, NullWritable.get());
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Total Sort app");
        //将小表加载到缓存中。
        job.addCacheFile(new URI(args[0]));
        job.setJarByClass(MapJoinMapper.class);
        //1.1 设置输入目录和设置输入数据格式化的类
        FileInputFormat.setInputPaths(job, new Path(args[1]));
        job.setInputFormatClass(TextInputFormat.class);

        //1.2 设置自定义Mapper类和设置map函数输出数据的key和value的类型
        job.setMapperClass(MapJoinMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        //1.3 设置reduce数量
        job.setNumReduceTasks(1);
        //设置实现了reduce函数的类
        job.setReducerClass(MapJoinReducer.class);

        //设置reduce函数的key值
        job.setOutputKeyClass(Text.class);
        //设置reduce函数的value值
        job.setOutputValueClass(NullWritable.class);

        // 判断输出路径是否存在，如果存在，则删除
        Path mypath = new Path(args[2]);
        FileSystem hdfs = mypath.getFileSystem(conf);
        if (hdfs.isDirectory(mypath)) {
            hdfs.delete(mypath, true);
        }

        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {

        int exitCode = ToolRunner.run(new MapJoin(), args);
        System.exit(exitCode);
    }
}
