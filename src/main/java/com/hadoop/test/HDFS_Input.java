package com.hadoop.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
//import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.net.URI;

public class HDFS_Input {

	public void downlocalByStream() {
		Configuration conf = new Configuration();
		FileSystem fs = null;
		try {
			fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), conf, "hadoop");
			FSDataInputStream fis = fs.open(new Path("/user"));

			FileOutputStream fos = new FileOutputStream(new File("e:/use02.txt"));

			IOUtils.copyBytes(fis, fos, conf);
			IOUtils.closeStream(fis);
			IOUtils.closeStream(fos);
			IOUtils.closeStream(fs);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	//@Test
	public void uplocalByStream() {
		Configuration conf = new Configuration();
		FileSystem fs = null;
		try {
			fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), conf, "hadoop");
			FSDataOutputStream fos = fs.create(new Path("/file01"));

			FileInputStream fis = new FileInputStream(new File("e:/use02.txt"));

			IOUtils.copyBytes(fis, fos, conf);
			IOUtils.closeStream(fis);
			IOUtils.closeStream(fos);
			IOUtils.closeStream(fs);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	// ��λ���ȡ
	//@Test
	public void readFileSeek() {
		Configuration conf = new Configuration();
		FileSystem fs = null;
		try {
			fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), conf, "hadoop");
			FSDataInputStream fis = fs.open(new Path("/user01/hadoop.zip"));

			FileOutputStream fos = new FileOutputStream(new File("e:/hadoop.zip.part1"));
			byte[] bys = new byte[1024];
			for (int i = 0; i < 1024 * 128; i++) {
				fis.read(bys);
				fos.write(bys);
			}

			IOUtils.closeStream(fis);
			IOUtils.closeStream(fos);
			IOUtils.closeStream(fs);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	// �ڶ����ȡ
//	@Test
	public void readFileSeek02() {
		Configuration conf = new Configuration();
		FileSystem fs = null;
		try {
			fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), conf, "hadoop");
			FSDataInputStream fis = fs.open(new Path("/user01/hadoop.zip"));
			fis.seek(1024 * 1024 * 128);
			// fis.seekToNewSource(1024*1024*128);
			FileOutputStream fos = new FileOutputStream(new File("e:/hadoop.zip.part2"));
			// 5 ���ĶԿ�
			IOUtils.copyBytes(fis, fos, conf);

			IOUtils.closeStream(fis);
			IOUtils.closeStream(fos);
			IOUtils.closeStream(fs);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
