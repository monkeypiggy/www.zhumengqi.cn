package cn.zhumengqi.es.util;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

/**
 * Created by zhumengqi on 17-3-6.
 */
public class SearchResult {
    private String code;
    private JSONArray messageArray;

    public SearchResult(){};
    public SearchResult(String code,String content) {
        this.code = code;
        this.messageArray = new JSONArray();
    }

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }

    public JSONArray getMessageArray() {
        return messageArray;
    }

    public void setMessageArray(JSONArray messageArray) {
        this.messageArray = messageArray;
    }
    public JSONObject toJsonObject(SearchResult searchResult){
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("code", searchResult.getCode());
        jsonObject.put("data", searchResult.getMessageArray());
        return jsonObject;
    }
}
