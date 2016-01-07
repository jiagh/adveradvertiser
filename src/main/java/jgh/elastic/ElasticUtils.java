package jgh.elastic;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.bucket.filters.Filters;

public class ElasticUtils {
    static Client client;

    public static void getClient() {
	try {
	    Settings settings = Settings.settingsBuilder().put("cluster.name", "ESC_F").build();
	    client = TransportClient.builder().settings(settings).build().addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("172.16.80.2"), 9300));
	} catch (UnknownHostException e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
	}
    }

    public static void createIndexResponse(String indexname, String type) {
	String json = "{" + "\"user\":\"restart\"," + "\"postDate\":\"2013-01-30\"," + "\"message\":\"test out Elasticsearch\"" + "}";
	IndexResponse response = client.prepareIndex(indexname, type).setSource(json).execute().actionGet();
	System.out.println("id::::" + response.getId());
    }

    public static void queryIndexById() {
	GetResponse response = client.prepareGet("testindex", "testType", "AVIWb0xfwAmorHHHqjAD").setOperationThreaded(false).execute().actionGet();
    }

    public static void queryIndex() {
	SearchResponse response = client.prepareSearch("testindex").setTypes("testType").setSearchType(SearchType.DFS_QUERY_AND_FETCH)
		.setQuery(QueryBuilders.commonTermsQuery("message", "out")) // Query
		.setFrom(0).setSize(60).setExplain(true).execute().actionGet();
	SearchHits searchHists = response.getHits();
	for (SearchHit hit : searchHists) {
	    System.out.println(hit.getSource().get("user") + ":::" + hit.getId());
	}
    }
    
    public static void queryIndexWithFilter() {
	SearchResponse response = client.prepareSearch("testindex").setTypes("testType").setSearchType(SearchType.DFS_QUERY_AND_FETCH)
		.setQuery(QueryBuilders.commonTermsQuery("message", "out")) // Query
		//.setPostFilter(QueryBuilders.rangeQuery("age").from(12).to(18)) //范围查询 int或double
		.setPostFilter(QueryBuilders.regexpQuery("message", ".*?ing*?"))
		.setFrom(0).setSize(60).setExplain(true).execute().actionGet();
	SearchHits searchHists = response.getHits();
	for (SearchHit hit : searchHists) {
	    System.out.println(hit.getSource().get("user") + ":::" + hit.getId());
	}
    }

    public static void deleteIndexById() {
	client.prepareDelete("testindex", "testType", "AVIWVaKewAmorHHHqiOp").execute().actionGet();
    }

    public static void deleteIndexByType() {
	client.prepareDelete().setIndex("testindex").setType("testType").execute().actionGet();
    }

    public static void queryIndexWithMulti() {
	MultiGetResponse multiGetItemResponses = client.prepareMultiGet().add("testindex", "testType", "AVIWdfd4wAmorHHHqjM3").add("testindex", "testType", "AVIaIq7bwAmorHHHq_up")
		.get();

	for (MultiGetItemResponse itemResponse : multiGetItemResponses) {
	    GetResponse response = itemResponse.getResponse();
	    if (response.isExists()) {
		String json = response.getSourceAsString();
		System.out.println(json);
	    }
	}
    }

    public static void queryIndexWithScroll() {
	QueryBuilder qb = QueryBuilders.commonTermsQuery("user", "kimchy");
	SearchResponse scrollResp =client.prepareSearch("testindex").setSearchType(SearchType.QUERY_AND_FETCH).setScroll(new TimeValue(30000)).setQuery(qb).setSize(2).execute().actionGet();
	for(SearchHit hit:scrollResp.getHits().getHits()){
	    System.out.println(hit.getSource().get("user") + ":::" + hit.getId());
	}
    }
    
    

    public static void main(String args[]) {
	getClient();
	 createIndexResponse("testindex", "testType"); //AVIaIq7bwAmorHHHq_up
	// queryIndexById();
	// deleteIndexById();
//	queryIndexWithMulti();
//	queryIndexWithScroll();
	queryIndexWithFilter();
	// kimchy:::AVIWdfd4wAmorHHHqjM3
	// kimchy:::AVIWeUp2wAmorHHHqjTb
	// restart:::AVIaIq7bwAmorHHHq_up
	// kimchy:::AVIWWNmPwAmorHHHqiU6
	// kimchy:::AVIWeKSHwAmorHHHqjSO
    }
}
