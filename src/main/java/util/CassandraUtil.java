package util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import config.Config;

public class CassandraUtil {

	private Cluster cluster;
	private static BufferedReader reader;

	public void connect(String node) {
		cluster = Cluster.builder().addContactPoint(node).build();
		Metadata metadata = cluster.getMetadata();
		System.out.printf("Connected to cluster: %s\n", metadata.getClusterName());
		for (Host host : metadata.getAllHosts()) {
			System.out.printf("Datacenter: %s; Host: %s; Rack: %s\n", host.getDatacenter(), host.getAddress(), host.getRack());
		}
	}

	public Session getSession() {
		Session session = cluster.connect();
		return session;
	}

	public void close() {
		cluster.close();
	}

	public static void main(String[] args) throws IOException {

		CassandraUtil client = new CassandraUtil();
		client.connect("192.168.8.1");
		Session session = client.getSession();

		PreparedStatement statement = session.prepare("SELECT * FROM dmp.page_img_info WHERE page_url = ? ");
		BoundStatement boundStatement = new BoundStatement(statement);

		File file = new File("d://part-00000");
		FileWriter fw = new FileWriter("d://result");

		reader = new BufferedReader(new FileReader(file));
		String tempString = null;
		int count = 0;
		while ((tempString = reader.readLine()) != null) {

			tempString = tempString.replaceAll("\\[|\\]", "");
			String tmp[] = tempString.split(",");
			ResultSet rs = session.execute(boundStatement.bind(tmp[1]));
			if (rs.all().size() == 0) {
				System.out.println(count++);
				fw.write(tmp[0] + "\t" + tmp[2] + Config.NEW_LINE);
			}
		}

		fw.flush();
		fw.close();

		client.close();
	}
}
