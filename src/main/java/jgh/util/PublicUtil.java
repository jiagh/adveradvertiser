package jgh.util;

import java.io.IOException;
import java.io.Serializable;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import config.Config;

@SuppressWarnings("serial")
public class PublicUtil implements Serializable {

    public static FileSystem getFs() throws IOException {
	return FileSystem.get(URI.create(Config.NAMENODE_ADDRESS), new Configuration());
    }
    public static void mkdirHDFSFolder() throws IOException {
	FileSystem fs = getFs();
	if (!fs.exists(new Path(Config.HDFS_LOG_BAK)))
	    fs.mkdirs(new Path(Config.HDFS_LOG_BAK));
	if (!fs.exists(new Path(Config.HDFS_LOG_MERGE_WORK)))
	    fs.mkdirs(new Path(Config.HDFS_LOG_MERGE_WORK));

    }
}
