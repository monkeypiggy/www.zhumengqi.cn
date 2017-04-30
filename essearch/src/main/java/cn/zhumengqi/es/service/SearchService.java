package cn.zhumengqi.es.service;

import cn.zhumengqi.es.util.*;

import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.net.URLDecoder;
import java.net.UnknownHostException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.Operator;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.springframework.stereotype.Component;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

/**
 * Created by zhumengqi on 17-4-30.
 */
@Component
public class SearchService {

    private TransportClient client;
    private static final Logger logger = LogManager.getLogger(SearchService.class);

    //	构造方法，创建与Elasticsearch通信的Client客户端。思考：是否开销大，整理到Service中，重用Client
    Config esConfig = ConfigFactory.load();
    SearchResult searchResult = new SearchResult();

    public SearchService() {
        try {
            String CLIENT_ADDRESS = esConfig.getString("elasticsearch.transport.address").split(":")[0];
            Integer CLIENT_PORT = Integer.parseInt(esConfig.getString("elasticsearch.transport.address").split(":")[1]);
            String CLUSTER_NAME = esConfig.getString("elasticsearch.cluster.name");
            Settings settings = Settings.builder()
                    .put("cluster.name", CLUSTER_NAME).build();
            client = new PreBuiltTransportClient(settings)
                    .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(CLIENT_ADDRESS), CLIENT_PORT));
        } catch (UnknownHostException e) {
            logger.info("TransportClient 创建异常，详细信息:");
            e.printStackTrace();
        }
    }

    public JSONObject getESTotal(String gid, String startDate, String endDate, String dept,
                                 String sid, String userType, String userValue, String type, int patch) throws
            Exception {
        ArrayList<String> ES_INDICES = genIndicesDate(gid, startDate, endDate);

        String fuzzyText;
        int SIZE = 10000;
        int FROM = patch * 10000;
        fuzzyText = convertValue(userType, userValue);
        logger.info("查询内容为：" + userType + ":" + userValue);

        ArrayList<String> validIndices = getExistsIndices();
        //计算查找时间与实际存在的时间的交集
        validIndices.retainAll(ES_INDICES);
        if (validIndices.isEmpty()) {
            logger.warn("所选日期范围内无数据");
            return new SearchResult("-1","No data").toJsonObject(searchResult);
        }

        logger.info("from is:" + FROM + ", size is : " + SIZE);
        SearchResponse searchResponse;
        try {
            //          由ArrayList<String>转化为String[]，不知是否可以再做优化，减少这一步。
            logger.info("validIndices are: " + validIndices);
            //            以下一步可以简化与否
            String[] indices = validIndices.toArray(new String[validIndices.size()]);
            //          暂时返回上限10000个，否则可能导致数据量太大，响应时间太长。
            if (sid.equals("all") && dept.equals("all")) {
                searchResponse = client.prepareSearch(indices)
                        .setTypes(type)
                        .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                        .setQuery(QueryBuilders.boolQuery()
                                .must(QueryBuilders.queryStringQuery(fuzzyText)
                                        .defaultOperator(Operator.AND)
                                        .defaultField(userType)))
                        .setFrom(FROM).setSize(SIZE).setExplain(true)
                        .execute()
                        .actionGet();
                logger.info("sid and dept == all");
            } else if (dept.equals("all")) {
                searchResponse = client.prepareSearch(indices)
                        .setTypes(type)
                        .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                        .setQuery(QueryBuilders.boolQuery()
                                .must(QueryBuilders.queryStringQuery(fuzzyText)
                                        .defaultOperator(Operator.AND)
                                        .defaultField(userType)))
                        .setPostFilter(QueryBuilders.termQuery("sid", sid))
                        .setFrom(FROM).setSize(SIZE).setExplain(true)
                        .execute()
                        .actionGet();

                logger.info("only dept == all");
            }
            //          兼容例如dept="a,b,c"
            else if (dept.contains(",") && sid.equals("all")) {
                String[] depts = dept.split(",");
                List list = Arrays.asList(depts);
                searchResponse = client.prepareSearch(indices)
                        .setTypes(type)
                        .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                        .setQuery(QueryBuilders.boolQuery()
                                .must(QueryBuilders.queryStringQuery(fuzzyText)
                                        .defaultOperator(Operator.AND)
                                        .defaultField(userType))
                        )
                        .setPostFilter(QueryBuilders.termsQuery("dept", list))
                        .setFrom(FROM).setSize(SIZE).setExplain(true)
                        .execute()
                        .actionGet();

                logger.info("sid == all, dept == " + dept);
            } else if (sid.equals("all")) {
                searchResponse = client.prepareSearch(indices)
                        .setTypes(type)
                        .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                        .setQuery(QueryBuilders.boolQuery()
                                .must(QueryBuilders.queryStringQuery(fuzzyText)
                                        .defaultOperator(Operator.AND)
                                        .defaultField(userType)))

                        .setPostFilter(QueryBuilders.termQuery("dept", dept))
                        .setFrom(FROM).setSize(SIZE).setExplain(true)
                        .execute()
                        .actionGet();
                logger.info("only sid == all");
            } else {
                searchResponse = client.prepareSearch(indices)
                        .setTypes(type)
                        .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                        .setQuery(QueryBuilders.boolQuery()
                                .must(QueryBuilders.queryStringQuery(fuzzyText).defaultOperator(Operator.AND).defaultField
                                        (userType))
                                .must(QueryBuilders.matchQuery("sid", sid))
                                .must(QueryBuilders.matchQuery("dept", dept)))
                        .setFrom(FROM).setSize(SIZE).setExplain(true)
                        .execute()
                        .actionGet();
                logger.info("sid and dept all != all");
            }
            logger.info("总匹配数量：" + searchResponse.getHits().getTotalHits());
            searchResult = genResult(searchResponse);
        } catch (Exception e) {
            logger.error("Get Response 异常: 或许获取参数时接收到了包含特殊字符的内容； 异常信息：");
            logger.error(e);
            return new SearchResult("-1","接收参数存在异常").toJsonObject(searchResult);
        }

        return searchResult.toJsonObject(searchResult);
    }

    // 接收es返回的数据，解析、分割，包装为前端接收的对象
    private SearchResult genResult(SearchResponse searchResponse){
        SearchHits hits = searchResponse.getHits();
        JSONArray content = new JSONArray();
        for (SearchHit searchHit : hits) {
            JSONObject messageJson = splitMessage(searchHit);
            content.add(messageJson);
        }
        searchResult.setCode("0");
        searchResult.setMessageArray(content);
        return searchResult;
    }

    //  若userType = rolename，则进行模糊搜索
    private String convertValue( String userType, String userValue){
        String fuzzyText = "*" + userValue + "*";
        if (!userType.equals("rolename")) {
            fuzzyText = userValue;
        } else {
            if (userValue.contains("[") || userValue.contains("]") || userValue.contains(":")) {
                logger.info("收到包含[或]的内容:" + userValue + "； 进行转义");
                userValue = userValue.replaceAll(":", "\\\\:");
                userValue = userValue.replaceAll("\\[", "\\\\[");
                userValue = userValue.replaceAll("]", "\\\\]");
                fuzzyText = "*" + userValue + "*";
            }
        }
        return fuzzyText;
    }
    private JSONObject splitMessage(SearchHit messageHit) {
        String[] kvArray,kv ;
        String key, value, message = messageHit.getSource().get("message").toString();
        kvArray = message.split("&");
        JSONObject jsonMessage = new JSONObject();
        for (String each : kvArray) {
            kv = each.split("=");
            if (kv.length != 2)
                continue;
            key = kv[0];
            value = kv[1];
            if (key.equals("time")) {
                try {
                    value = CusDataUtil.timeStampCovertStand(value);
                } catch (Exception e) {
                    value = kv[1];
                }
                jsonMessage.put(key, value);
            } else {
                try {
                    value = URLDecoder.decode(URLDecoder.decode(value, "utf-8"));
                } catch (UnsupportedEncodingException e) {
                    logger.error("解析message的values时转码失败");
                    logger.error(e);
                }
                jsonMessage.put(key, value);
            }
        }
        return jsonMessage;
    }

    //  根据查询时间，生成对应的索引id列表，以确定搜索范围。
    private ArrayList<String> genIndicesDate(String gid, String startDateString, String endDateString) throws
            Exception {
        //      将日期中的-替换为. TODO:可以在CusDataUtil类中修改这个方法，或者增加一个专用的方法
        startDateString = CusDataUtil.timeStampCovertES(startDateString).substring(0, 10);//.replace("-", ".");
        endDateString = CusDataUtil.timeStampCovertES(endDateString).substring(0, 10);//.replace("-", ".");
        ArrayList<String> indicesList = new ArrayList<String>();

        //      如果起止时间相等，则是同一天，拼接并返回该索引即可
        if (startDateString.equals(endDateString)) {
            String index = gid + "-" + startDateString;
            String index2 = "kbzy" + "-" + startDateString;
            indicesList.add(index);
            indicesList.add(index2);
            return indicesList;
        }
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy.MM.dd");
        Date startDate, endDate;
        //      利用java中的日历，产生startDate与endDate之间的所有日子
        try {
            startDate = sdf.parse(startDateString);
            endDate = sdf.parse(endDateString);
            Calendar calendar = Calendar.getInstance();
            calendar.setTime(startDate);
            while (calendar.getTime().before(endDate)) {
                indicesList.add(gid + "-" + sdf.format(calendar.getTime()));
                indicesList.add("kbzy" + "-" + sdf.format(calendar.getTime()));
                calendar.add(Calendar.DAY_OF_MONTH, 1);
            }
            calendar.add(Calendar.DAY_OF_MONTH, 0);
            indicesList.add(gid + "-" + sdf.format(calendar.getTime()));
            indicesList.add("kbzy" + "-" + sdf.format(calendar.getTime()));
        } catch (ParseException e) {
            e.printStackTrace();
        }
        logger.info("拼接的时间范围的参数为：" + indicesList.toString());
        return indicesList;
    }

    private ArrayList<String> getExistsIndices() {
        //      TODO:添加耗时记录，便于评估时间开销
        ArrayList<String> validIndices = new ArrayList<String>(Arrays.asList(client.admin().cluster().prepareState().get
                ().getState()
                .getMetaData()
                .getConcreteAllIndices()));
        return validIndices;
    }
}
