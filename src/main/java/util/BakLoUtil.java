package util;

import java.io.IOException;

import org.apache.hadoop.fs.Path;

public class BakLoUtil {

	// 青岛节点外网地址： http://oss-cn-qingdao.aliyuncs.com
	// 青岛节点内网地址： http://oss-cn-qingdao-internal.aliyuncs.com
	// 北京节点外网地址：http://oss-cn-beijing.aliyuncs.com
	// 北京节点内网地址：http://oss-cn-beijing-internal.aliyuncs.com
	// 杭州节点外网地址： http://oss-cn-hangzhou.aliyuncs.com
	// 杭州节点内网地址： http://oss-cn-hangzhou-internal.aliyuncs.com
	// 香港节点外网地址： http://oss-cn-hongkong.aliyuncs.com
	// 香港节点内网地址： http://oss-cn-hongkong-internal.aliyuncs.com
	// 深圳节点外网地址： http://oss-cn-shenzhen.aliyuncs.com
	// 深圳节点内网地址： http://oss-cn-shenzhen-internal.aliyuncs.com

	// 北京机房OSS地址
	// private OSSClient client = new OSSClient(Config.ossEndpoint,
	// Config.ossAccessKeyId, Config.ossAccessKeySecret);

	public void ossAndHdfsAndLocalReqBak(String year, String month, String day, String hour, Path p)
			throws IOException {

		// if (new File(Config.localBakLogsReq).exists() == false)
		// new File(Config.localBakLogsReq).mkdirs();
		//
		// if (new File(Config.localBakLogsReq + "/" + year + "/").exists() ==
		// false)
		// new File(Config.localBakLogsReq + "/" + year + "/").mkdir();
		//
		// if (new File(Config.localBakLogsReq + "/" + year + "/" + month +
		// "/").exists() == false)
		// new File(Config.localBakLogsReq + "/" + year + "/" + month +
		// "/").mkdir();
		//
		// if (new File(Config.localBakLogsReq + "/" + year + "/" + month + "/"
		// + day + "/").exists() == false)
		// new File(Config.localBakLogsReq + "/" + year + "/" + month + "/" +
		// day + "/").mkdir();
		//
		// if (new File(Config.localBakLogsReq + "/" + year + "/" + month + "/"
		// + day + "/" + hour + "/").exists() == false)
		// new File(Config.localBakLogsReq + "/" + year + "/" + month + "/" +
		// day + "/" + hour + "/").mkdir();

		// 本地 备份
		// downFileAndCompression(p.toString(), Config.localBakLogsReq + "/" +
		// year + "/" + month + "/" + day + "/" + hour + "/" + p.getName());

		// // 创建HDFS连接
		// FileSystem fs = FileSystem.get(URI.create(Config.nameNodeIp + ":" +
		// Config.nameNodePort), new Configuration());
		//
		// if (fs.exists(new Path(Config.hdfsBakLogsReq + "/" + year + "/")))
		// fs.mkdirs(new Path(Config.hdfsBakLogsReq + "/" + year + "/"));
		//
		// if (fs.exists(new Path(Config.hdfsBakLogsReq + "/" + year + "/" +
		// month + "/")))
		// fs.mkdirs(new Path(Config.hdfsBakLogsReq + "/" + year + "/" + month +
		// "/"));
		//
		// if (fs.exists(new Path(Config.hdfsBakLogsReq + "/" + year + "/" + "/"
		// + day + "/")))
		// fs.mkdirs(new Path(Config.hdfsBakLogsReq + "/" + year + "/" + day +
		// "/"));
		//
		// if (fs.exists(new Path(Config.hdfsBakLogsReq + "/" + year + "/" + "/"
		// + day + "/" + hour + "/")))
		// fs.mkdirs(new Path(Config.hdfsBakLogsReq + "/" + year + "/" + day +
		// "/" + hour + "/"));
		//
		// fs.rename(p, new Path(Config.localBakLogsReq + "/" + year + "/" +
		// month + "/" + day + "/" + hour + "/" + p.getName()));

		// 在上传到HDFS 按照年月日存储
		// fs.copyFromLocalFile(new Path(Config.localBakLogsReq + "/" + year +
		// "/" + month + "/" + day + "/" + hour + "/" + p.getName()), new
		// Path(Config.hdfsBakLogsReq + "/" + year + "/" + month + "/" + day +
		// "/" + hour + "/" + p.getName()));

		// 删除原始文件只保留压缩文件
		// new File(Config.localBakLogsReq + "/" + year + "/" + month + "/" +
		// day + "/" + hour + "/" + p.getName()).delete();

		// --
		// Complete.synDelThread();

		// // 开始 OSS 备份
		// File file = new File(Config.localBakLogsReq + "/" + year + "/" +
		// month + "/" + day + "/" + hour + "/" + p.getName() + ".zip");
		// //
		// InputStream content = new FileInputStream(file);
		// //
		// ObjectMetadata objectMeta = new ObjectMetadata();
		// // 必须设置ContentLength
		// objectMeta.setContentLength(5 * 1024 * 1024 * 1024L);
		//
		// try {
		// client.putObject("cogtu-offline-log", "reqlog/" + year + "/" + month
		// + "/" + day + "/" + hour + "/" + p.getName() + ".zip", content,
		// objectMeta);
		// } finally {
		// content.close();
		// }

	}

	// // 下载到本地目录并压缩
	// public static void downFileAndCompression(String inpath, String outpath)
	// throws IOException {
	//
	// // 创建HDFS连接
	// FileSystem fs = FileSystem.get(URI.create(Config.nameNodeIp + ":" +
	// Config.nameNodePort), new Configuration());
	// // 读取文件
	// InputStream is = fs.open(new Path(inpath));
	// // 保存到本地
	// IOUtils.copyBytes(is, new FileOutputStream(new File(outpath)), 2048,
	// true);
	// // 压缩
	// ZipTool.zipSingleFile(outpath, outpath + ".zip");
	//
	// }
}
