package com.liaofeng_liujie.spring_mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.util.StreamUtils;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestPart;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.attribute.PosixFilePermission;
import java.text.SimpleDateFormat;
import java.util.*;
import com.jcraft.jsch.*;

import static com.sun.jmx.remote.internal.IIOPHelper.connect;

@SpringBootApplication
@RestController
public class WordCountApplication {
    public static void main(String[] args) {
        SpringApplication.run(WordCountApplication.class, args);
    }
    @Value("D:\\编程\\hadoop项目\\spring_mapreduce\\data_work\\") // 获取系统的临时目录
    private String tempDir;
    public static void connect(String resultPath,String linuxPath,String timestamp) {
        String host = "hadoop102";
        int port = 22;
        String username = "liaofeng";
        String password = "1015lf..";

        try {
            JSch jsch = new JSch();
            Session session = jsch.getSession(username, host, port);
            session.setPassword(password);
            session.setConfig("StrictHostKeyChecking", "no");
            session.connect();

            Channel channel = session.openChannel("shell");
            channel.connect();
            // 在此处执行命令
            ((ChannelShell) channel).setPty(true);
            ((ChannelShell) channel).setPtyType("vt102");

            // 设置输入输出流
            InputStream inputStream = channel.getInputStream();
            OutputStream outputStream = channel.getOutputStream();

            // 向远程服务器发送命令
            String command = "hadoop fs -get " + resultPath + " " + linuxPath + timestamp + "\n";
            outputStream.write(command.getBytes());
            outputStream.flush();
            // 读取远程服务器的输出
            byte[] buffer = new byte[1024];
            while (true) {
                if (inputStream.available() > 0) {
                    int bytesRead = inputStream.read(buffer);
                    if (bytesRead < 0) {
                        break;
                    }
                    String output = new String(buffer, 0, bytesRead);
                    System.out.print(output);
                } else {
                    try {
                        Thread.sleep(3000); // 等待1秒继续检查输出
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }

            // 关闭连接
            channel.disconnect();
            session.disconnect();
        } catch (JSchException | IOException e) {
            e.printStackTrace();
        }
    }
    @PostMapping("/mapreduce")
    public ResponseEntity<Map<String, Integer>> performMapReduce(@RequestPart("file") MultipartFile file) {
        try {
            // 将上传的文件保存到HDFS中
            Configuration conf = new Configuration();
            conf.set("fs.defaultFS","hdfs://hadoop102:8020");
            FileSystem fs = FileSystem.get(conf);

            Path inputPath = new Path("/spring_work/" + file.getOriginalFilename());
            fs.copyFromLocalFile(new Path(tempDir + file.getOriginalFilename()), inputPath);

            // 设置MapReduce作业的配置
            Job job = Job.getInstance(conf, "WordCount");
            job.setJarByClass(WordCountApplication.class);
            job.setMapperClass(WordCountMapper.class);
            job.setCombinerClass(WordCountReducer.class);
            job.setReducerClass(WordCountReducer.class);
            //设置Key Value
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(LongWritable.class);
            //输入数据使用mapreduce
            FileInputFormat.addInputPath(job, inputPath);

            //根据时间戳存储文件保证不会重复
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd_HHmmss");
            String timestamp = dateFormat.format(new Date());

            // 设置MapReduce作业的输出路径
            Path outputPath = new Path("/spring_work/output/" + timestamp);
            FileOutputFormat.setOutputPath(job, outputPath);

            // 执行MapReduce作业
            job.waitForCompletion(true);

            // 从HDFS中读取统计结果
            Path resultPath = new Path(outputPath, "part-r-00000");
//            Path linuxPath = new Path("/opt/module/data_work/hdfs/output/");
//            connect(resultPath.toString(),linuxPath.toString(),timestamp);
            InputStream resultStream = fs.open(resultPath);
            String result = StreamUtils.copyToString(resultStream, StandardCharsets.UTF_8);
            resultStream.close();

            // 解析统计结果并返回给前端页面
            Map<String, Integer> wordCountMap = parseResult(result);
            return new ResponseEntity<>(wordCountMap, HttpStatus.OK);
        } catch (Exception e) {
            e.printStackTrace();
            return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    private Map<String, Integer> parseResult(String result) {
        Map<String, Integer> wordCountMap = new HashMap<>();
        String[] lines = result.split("\n");
        for (String line : lines) {
            String[] parts = line.split("\t");
            String word = parts[0];
            int count = Integer.parseInt(parts[1]);
            wordCountMap.put(word, count + wordCountMap.getOrDefault(word,0));
        }
        return wordCountMap;
    }
}
