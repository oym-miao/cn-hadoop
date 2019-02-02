package org.hadoop.run;




import org.hadoop.files.Files;
import org.hadoop.mrs.wordcount.WordCountMR;

/**
 *
 * Created by shirukai on 2017/11/8.
 */
public class RunHadoop {
    public static void main(String[] args) {
        //D:\hadoopProject\mapreduce\wordcount\out\artifacts\wordcount_jar
        //创建目录
/*
    String folderName = "/output";
     // Files.mkdirFolder(folderName);
      Files.deleteFile(folderName);
*/


       //创建word_input目录
         String folderName = "/word_input";
        Files.mkdirFolder(folderName);
        System.out.println("创建" + folderName + "成功");
        //创建word_input目录
        folderName = "/word_output";
        Files.mkdirFolder(folderName);
        System.out.println("创建" + folderName + "成功");

        //上传文件
        String localPath = "D:\\hadoop\\upload\\";
        String fileName = "words.txt";
        String hdfsPath = "/word_input/";
        Files.uploadFile(localPath, fileName, hdfsPath);
        System.out.println("上传" + fileName + "成功");

        //执行wordcount
        int result = -1;
        try{
            result = new WordCountMR().run();
        }catch (Exception e){
            System.out.println(e.getMessage());
        }

        if (result == 1) {
            System.out.println("");
            //成功后下载文件到本地
            String downPath = "/word_output/";
            String downName ="part-r-00000";
            String savePath = "D:\\hadoop\\download\\";
            Files.getFileFromHadoop(downPath,downName,savePath);
            System.out.println("执行成功并将结果保存到"+savePath);
        } else {
            System.out.println("执行失败");
        }
    }
}

