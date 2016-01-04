import java.util.ArrayList;

public class SCP {

	public static void main(String[] args) throws ClassNotFoundException {

		System.out.println("asdasd|-|123123".replaceAll("\\|-\\|", ""));

		String scp_hadoop_config = "scp -r /usr/local/hadoop/etc/hadoop/ root@tihuan:/usr/local/hadoop/etc/";
		String scp_hadoop_config_ = "scp -r /root/cdh5.4.4/hadoop-2.6.0-cdh5.4.4/etc/hadoop/ root@tihuan:/root/cdh5.4.4/hadoop-2.6.0-cdh5.4.4//etc/";

		String scp_hosts = "scp -r /etc/hosts root@tihuan:/etc/";

		ArrayList<String> name = new ArrayList<String>();

		// 测试
		// name.add("iZ25hc55c9lZ");
		// name.add("iZ25e12rel0Z");
		// name.add("iZ25220fqoqZ");
		// name.add("iZ25yygtbj3Z");

		// 正式
		name.add("prod-infra-hadoop01");
		name.add("prod-infra-hadoop02");
		name.add("prod-infra-hadoop03");
		name.add("prod-infra-hadoop04");
		name.add("prod-infra-hadoop05");

		for (int i = 0; i < name.size(); i++) {

			System.out.println(scp_hadoop_config.replace("tihuan", name.get(i)));

		}

	}
}
