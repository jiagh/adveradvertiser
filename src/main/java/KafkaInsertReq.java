import java.util.ArrayList;
import java.util.Properties;
import java.util.Random;

import config.Config;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class KafkaInsertReq {

	public static void main(String[] args) {
		Properties props = new Properties();
		props.put("zk.connect", Config.KAFKA_ZOOKEEPER_ADDRESS);
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		props.put("metadata.broker.list", Config.KAFKA_BROKER_ADDRESS);
		props.put("request.required.acks", "1");

		ProducerConfig config = new ProducerConfig(props);
		Producer<String, String> producer = new Producer<String, String>(config);

		Random r = new Random(5000000);
		ArrayList<KeyedMessage<String, String>> al = new ArrayList<KeyedMessage<String, String>>();

		for (int i = 0; i < Integer.parseInt(args[0]); i++) {

			System.out.println(i);

			String log = "{\"addrCode\":\"\",\"agent\":\"python-requests/2.8.1\",\"browser\":\"Python-requests|2\",\"city\":\"\",\"country\":\"局域网\",\"ip\":\"172.16.1.16\",\"os\":\"unknown\",\"pageUri\":\"http://games.sina.com.cn/ol/n/2015-09-08/fxhqtsx3623702.shtml\",\"province\":\"局域网\",\"publisherId\":2,\"reqType\":4,\"reqs\":[{\"advertisersId\":234,\"campaignId\":338,\"creativeId\":14,\"impId\":\"http://n.sinaimg.cn/transform/20150908/dFYu-fxhqhui4951485.JPG\",\"price\":3000,\"priceType\":\"CPM\",\"projectId\":1200,\"templateId\":14,\"campaignType\":1}],\"runTime\":7,\"sessionId\":\"e7aae22b-fff5-440f-81ba-9294148be6f3\",\"siteId\":3,\"slotId\":\"ct-00000000-0\",\"source\":\"req\",\"timestamp\":1449471025385,\"uid\":\"55f2b772.5170932\",\"v\":\"1.1\",\"vBalanceCostPrecent\":1}";

			KeyedMessage<String, String> data = new KeyedMessage<String, String>("Req", log);

			al.add(data);

			if (i % 10000 == 0) {
				producer.send(al);
				al = new ArrayList<KeyedMessage<String, String>>();
			}
		}

		if (al.size() > 0)
			producer.send(al);

		producer.close();

	}
}
