package jgh.elastic;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket;

public class TestElastic {
    static Client client;

    static {
	Settings settings = Settings.settingsBuilder().put("cluster.name", "ESC_F").build();
	try {
	    client = TransportClient.builder().settings(settings).build().addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("172.16.80.2"), 9300));
	} catch (UnknownHostException e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
	}
    }

    public static void addIndex() {
	List<String> dataList = JsonUtil.getInitJsonData();
	for (String s : dataList) {
	    client.prepareIndex("testindex", "user").setSource(s).execute().actionGet();
	}
    }

    public static void batchAddIndex(){
   	BulkRequestBuilder bulkRequest = client.prepareBulk();
   	List<String> dataList = JsonUtil.getInitJsonData();
   	for (String s : dataList) {
   	 bulkRequest.add(client.prepareIndex("testindex", "user").setSource(s));
	}
   	bulkRequest.execute().actionGet();
       }
    /**
     * 获取某个字段的所有值
     */
    public static void queryByAggregation() {
	SearchResponse sr = client.prepareSearch().setQuery(QueryBuilders.matchAllQuery()).addAggregation(AggregationBuilders.terms("query1").field("grade"))
		.addAggregation(AggregationBuilders.terms("query2").field("class")).execute().actionGet();
	Terms t1 = sr.getAggregations().get("query1");
	Terms t2 = sr.getAggregations().get("query2");
	for(Bucket bu:t2.getBuckets()){
	   System.out.println(bu.getKey()); 
	}
    }

    public static void main(String args[]) {
	new TestElastic();
//	addIndex();
	queryByAggregation();
	//batchAddIndex();
    }

}
