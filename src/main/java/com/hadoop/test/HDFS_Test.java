package com.hadoop.test;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IOUtils;
//import org.junit.Test;

public class HDFS_Test {
	
	//@Test
	public void copyFromLocalFile(){
		Configuration config=new Configuration();
		FileSystem fs=null;
		try {
			config.set("dfs.replication", "2");//���ø����������������û���������
			 fs=FileSystem.get(new URI("hdfs://hadoop102:9000"), config, "hadoop");
			 fs.copyFromLocalFile(new Path("d:\\hadoop.txt"), new Path("/user01/hadoop02.txt"));
			 
		} catch (Exception e) {
			e.printStackTrace();
		}finally {
			if(fs!=null){
				try {
					fs.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}
	//@Test
	public void get(){
		Configuration config=new Configuration();
		FileSystem fs=null;
		try {
			 fs=FileSystem.get(new URI("hdfs://hadoop102:9000"), config, "hadoop");
			 InputStream stream = fs.open(new Path("/user"));
			 IOUtils.copyBytes(stream, System.out, config);
		} catch (Exception e) {
			e.printStackTrace();
		}finally {
			if(fs!=null){
				try {
					fs.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}
	//@Test
	public void chmod(){
		Configuration config=new Configuration();
		FileSystem fs=null;
		try {
			 fs=FileSystem.get(new URI("hdfs://hadoop102:9000"), config, "hadoop");
			 fs.setPermission(new Path("/user01/hadoop01.txt"), new FsPermission("777"));
		} catch (Exception e) {
			e.printStackTrace();
		}finally {
			if(fs!=null){
				try {
					fs.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	//@Test
	public void update(){
		Configuration config=new Configuration();
		FileSystem fs=null;
		try {
			fs=FileSystem.get(new URI("hdfs://hadoop102:9000"), config, "hadoop");
			fs.rename(new Path("/user01/hadoop01.txt"), new Path("/user01/hadoop03.txt"));
		} catch (Exception e) {
			e.printStackTrace();
		}finally {
			if(fs!=null){
				try {
					fs.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}
//	@Test
	public void delete(){
		Configuration config=new Configuration();
		FileSystem fs=null;
		try {
			fs=FileSystem.get(new URI("hdfs://hadoop102:9000"), config, "hadoop");
			fs.delete(new Path("/user01"),false);
		} catch (Exception e) {
			e.printStackTrace();
		}finally {
			if(fs!=null){
				try {
					fs.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}
	//@Test
	public void listFiles(){
		Configuration config=new Configuration();
		FileSystem fs=null;
		try {
			fs=FileSystem.get(new URI("hdfs://hadoop102:9000"), config, "hadoop");
			RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(new Path("/"), true);
			while (iterator.hasNext()) {
				LocatedFileStatus next = iterator.next();
				// �������
				// �ļ�����
				Path path = next.getPath();
				System.out.println(path);
				String name = next.getPath().getName();
				System.out.println(name);
				// ����
				long len = next.getLen();
				System.out.println(len);
				// Ȩ��
				FsPermission permission = next.getPermission();
				System.out.println(permission);
				// ����
				String group = next.getGroup();
				System.out.println(group);
				// ��ȡ�洢�Ŀ���Ϣ
				BlockLocation[] blockLocations = next.getBlockLocations();
				for (int i = 0; i < blockLocations.length; i++) {
					System.out.println(blockLocations[i]);
				}
				System.out.println("-----------------------------------");
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		}finally {
			if(fs!=null){
				try {
					fs.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}
	//@Test
	public void isDirectoty(){
		FileSystem system =null;
		Configuration conf = new Configuration();
		try {
			system = FileSystem.get(new URI("hdfs://hadoop102:9000"), conf, "hadoop");
			FileStatus[] status = system.listStatus(new Path("/"));
			for (int i = 0; i < status.length; i++) {
				boolean file = status[i].isFile();
				if(file==true){
					System.out.println("f:"+status[i].getPath());
				}else{
					System.out.println("d:"+status[i].getPath().getName());
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}finally {
			try {
				if(system!=null)
				system.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	//@Test
	public void copuToFile(){
		FileSystem system =null;
		Configuration conf = new Configuration();
		try {
			system = FileSystem.get(new URI("hdfs://hadoop102:9000"), conf, "hadoop");
			system.copyToLocalFile(false, new Path("/user01/hadoop01.txt"), new Path("e:/use.txt"),true);
		} catch (Exception e) {
			e.printStackTrace();
		}finally {
			try {
				if(system!=null)
				system.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
}
