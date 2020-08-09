package com.lds.hdfs;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

public class HdfsClient {
	@Test
	public void testMkdirs() throws IOException, InterruptedException, URISyntaxException {

		// 1 获取文件系统
		Configuration configuration = new Configuration();
		// 配置在集群上运行
		// configuration.set("fs.defaultFS", "hdfs://hadoop102:9000");
		// FileSystem fs = FileSystem.get(configuration);

		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop101:9000"), configuration, "root");

		// 2 创建目录
		fs.mkdirs(new Path("/1108/daxian/banzhang"));

		// 3 关闭资源
		fs.close();
	}

	@Test
	public void testCopyFromLocalFile() throws IOException, InterruptedException, URISyntaxException {
		// 获取文件系统
		Configuration configuration = new Configuration();
		configuration.set("dfs.replication", "2");
		FileSystem fSystem = FileSystem.get(new URI("hdfs://hadoop101:9000"), configuration, "root");

		// 文件上传
		fSystem.copyFromLocalFile(new Path("d:/banzhang.txt"), new Path("/banzhang.txt"));

		// 关闭资源
		fSystem.close();

		System.out.println("over");
	}

	@Test
	public void testCopytoLocalFile() throws IOException, InterruptedException, URISyntaxException {
		// 获取文件系统
		Configuration configuration = new Configuration();
		FileSystem fSystem = FileSystem.get(new URI("hdfs://hadoop101:9000"), configuration, "root");

		// 执行下载操作
		fSystem.copyToLocalFile(false, new Path("/banzhang.txt"), new Path("d:/banzhang_1.txt"));

		// 关闭资源\
		fSystem.close();

	}

	@Test
	public void testDelete() throws IOException, InterruptedException, URISyntaxException {
		// 获取文件系统
		Configuration configuration = new Configuration();
		FileSystem fSystem = FileSystem.get(new URI("hdfs://hadoop101:9000"), configuration, "root");

		// 执行删除
		fSystem.delete(new Path("/1108"), true);

		// 关闭资源
		fSystem.close();

	}

	@Test
	public void testRename() throws URISyntaxException, IOException, InterruptedException {
		// 1 获取文件系统
		Configuration configuration = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "atguigu");

		// 2 修改文件名称
		fs.rename(new Path("/banzhang.txt"), new Path("/banhua.txt"));

		// 3 关闭资源
		fs.close();

	}

	@Test
	public void testListFiles() throws IOException, InterruptedException, URISyntaxException{

		// 1获取文件系统
		Configuration configuration = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "atguigu");

		// 2 获取文件详情
		RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);

		while(listFiles.hasNext()){
			LocatedFileStatus status = listFiles.next();

			// 输出详情
			// 文件名称
			System.out.println(status.getPath().getName());
			// 长度
			System.out.println(status.getLen());
			// 权限
			System.out.println(status.getPermission());
			// 分组
			System.out.println(status.getGroup());

			// 获取存储的块信息
			BlockLocation[] blockLocations = status.getBlockLocations();

			for (BlockLocation blockLocation : blockLocations) {

				// 获取块存储的主机节点
				String[] hosts = blockLocation.getHosts();

				for (String host : hosts) {
					System.out.println(host);
				}
			}

			System.out.println("-----------班长的分割线----------");
		}

		// 3 关闭资源
		fs.close();
	}

	@Test
	public void testListStatus() throws IOException, InterruptedException, URISyntaxException{

		// 1 获取文件配置信息
		Configuration configuration = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "atguigu");

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

	@Test
	public void putFileToHDFS() throws IOException, InterruptedException, URISyntaxException {

		// 1 获取文件系统
		Configuration configuration = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "atguigu");

		// 2 创建输入流
		FileInputStream fis = new FileInputStream(new File("e:/banhua.txt"));

		// 3 获取输出流
		FSDataOutputStream fos = fs.create(new Path("/banhua.txt"));

		// 4 流对拷
		IOUtils.copyBytes(fis, fos, configuration);

		// 5 关闭资源
		IOUtils.closeStream(fos);
		IOUtils.closeStream(fis);
		fs.close();
	}

	// 文件下载
	@Test
	public void getFileFromHDFS() throws IOException, InterruptedException, URISyntaxException{

		// 1 获取文件系统
		Configuration configuration = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "atguigu");

		// 2 获取输入流
		FSDataInputStream fis = fs.open(new Path("/banhua.txt"));

		// 3 获取输出流
		FileOutputStream fos = new FileOutputStream(new File("e:/banhua.txt"));

		// 4 流的对拷
		IOUtils.copyBytes(fis, fos, configuration);

		// 5 关闭资源
		IOUtils.closeStream(fos);
		IOUtils.closeStream(fis);
		fs.close();
	}
//-------定位文件读取------分块读取hdfs上大文件
@Test
public void readFileSeek1() throws IOException, InterruptedException, URISyntaxException{

	// 1 获取文件系统
	Configuration configuration = new Configuration();
	FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "atguigu");

	// 2 获取输入流
	FSDataInputStream fis = fs.open(new Path("/hadoop-2.7.2.tar.gz"));

	// 3 创建输出流
	FileOutputStream fos = new FileOutputStream(new File("e:/hadoop-2.7.2.tar.gz.part1"));

	// 4 流的拷贝
	byte[] buf = new byte[1024];

	for(int i =0 ; i < 1024 * 128; i++){
		fis.read(buf);
		fos.write(buf);
	}

	// 5关闭资源
	IOUtils.closeStream(fis);
	IOUtils.closeStream(fos);
	fs.close();
}
	@Test
	public void readFileSeek2() throws IOException, InterruptedException, URISyntaxException{

		// 1 获取文件系统
		Configuration configuration = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "atguigu");

		// 2 打开输入流
		FSDataInputStream fis = fs.open(new Path("/hadoop-2.7.2.tar.gz"));

		// 3 定位输入数据位置
		fis.seek(1024*1024*128);

		// 4 创建输出流
		FileOutputStream fos = new FileOutputStream(new File("e:/hadoop-2.7.2.tar.gz.part2"));

		// 5 流的对拷
		IOUtils.copyBytes(fis, fos, configuration);

		// 6 关闭资源
		IOUtils.closeStream(fis);
		IOUtils.closeStream(fos);
	}

}
