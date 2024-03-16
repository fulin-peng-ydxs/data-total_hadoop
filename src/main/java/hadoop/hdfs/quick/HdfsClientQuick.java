package hadoop.hdfs.quick;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.Test;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;

/**
 * hdf快速开始
 *
 * @author pengshuaifeng
 * 2024/3/10
 */
public class HdfsClientQuick{


    //创建目录
    @Test
    public void testMkdir() throws IOException, URISyntaxException,
            InterruptedException {
        // 1 获取文件服务器
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://linux1:8020"), configuration,"fulin");
        // 2 创建目录
        fs.mkdirs(new Path("/xiyou/huaguoshan/"));
        // 3 关闭资源
        fs.close();
    }

    //上传文件
    @Test
    public void testCopyFromLocalFile() throws IOException, InterruptedException, URISyntaxException {
        // 1 获取文件系统
        Configuration configuration = new Configuration();
        configuration.set("dfs.replication", "2");
        FileSystem fs = FileSystem.get(new URI("hdfs://linux1:8020"), configuration, "fulin");
        // 2 上传文件
        fs.copyFromLocalFile(new Path("/Users/pengshuaifeng/Downloads/笔记/随堂绘图.pdf"), new Path("/xiyou/huaguoshan"));
        // 3 关闭资源
        fs.close();
    }

    //下载文件
    @Test
    public void testCopyToLocalFile() throws IOException,
            InterruptedException, URISyntaxException{
        // 1 获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://linux1:8020"),
                configuration, "fulin");
        // 2 执行下载操作
        // boolean delSrc 指是否将原文件删除
        // Path src 指要下载的文件路径
        // Path dst 指将文件下载到的路径
        // boolean useRawLocalFileSystem 是否开启文件校验
        fs.copyToLocalFile(false, new
                        Path("/xiyou/huaguoshan/sunwukong.txt"), new Path("d:/sunwukong2.txt"),
                true);
        // 3 关闭资源
        fs.close();
    }

    //移动文件
    @Test
    public void testRename() throws IOException, InterruptedException,
            URISyntaxException{
        // 1 获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://linux1:8020"),
                configuration, "fulin");
        // 2 修改文件名称
        fs.rename(new Path("/xiyou/huaguoshan/sunwukong.txt"), new
                Path("/xiyou/huaguoshan/meihouwang.txt"));
        // 3 关闭资源
        fs.close();
    }

    //删除文件、目录
    @Test
    public void testDelete() throws IOException, InterruptedException,
            URISyntaxException{
        // 1 获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://linux1:8020"),
                configuration, "fulin");
        // 2 执行删除
        fs.delete(new Path("/xiyou"), true);
        // 3 关闭资源
        fs.close();
    }

    //查看文件详情
    @Test
    public void testListFiles() throws IOException, InterruptedException,
            URISyntaxException {
        // 1 获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://linux1:8020"),
                configuration, "fulin");
        // 2 获取文件详情
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"),
                true);
        while (listFiles.hasNext()) {
            LocatedFileStatus fileStatus = listFiles.next();
            System.out.println("========" + fileStatus.getPath() + "=========");
            System.out.println(fileStatus.getPermission()); // 权限
            System.out.println(fileStatus.getOwner()); // 文件所有者
            System.out.println(fileStatus.getGroup()); // 文件所在的组
            System.out.println(fileStatus.getLen()); // 文件的长度
            System.out.println(fileStatus.getModificationTime()); // 文件的修改时间
            System.out.println(fileStatus.getReplication()); // 文件的副本数
            System.out.println(fileStatus.getBlockSize());  // 文件的块大小
            System.out.println(fileStatus.getPath().getName()); // 文件名称
            BlockLocation[] blockLocations = fileStatus.getBlockLocations(); // 文件的块信息
            System.out.println(Arrays.toString(blockLocations));
        }
        // 3 关闭资源
        fs.close();
    }

    //判断是文件还是文件夹
    @Test
    public void testListStatus() throws IOException, InterruptedException,
            URISyntaxException{
        // 1 获取文件配置信息
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://linux1:8020"),
                configuration, "fulin");
        // 2 判断是文件还是文件夹
        FileStatus[] listStatus = fs.listStatus(new Path("/"));
        for (FileStatus fileStatus : listStatus) {
            // 如果是文件
            if (fileStatus.isFile()) {
                System.out.println("f:"+fileStatus.getPath().getName());
            }else {
                System.out.println("d:"+fileStatus.getPath().getName());
            }
        }
        // 3 关闭资源
        fs.close();
    }

}
