package cn.zhumengqi.es.controller;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.alibaba.fastjson.JSONObject;

/**
 * Created by zhumengqi on 17-4-30.
 */
@RestController
public class SearchController {
    private static final Logger logger = LogManager.getLogger(SearchController.class);

    @RequestMapping("/gs/handle")
    public void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        //      必要时引入SprintMVC框架
        //        TODO:设置req程和resp
        setReqResp(request, response);
        response.setContentType("text/json");
        response.setCharacterEncoding("utf-8");
        request.setCharacterEncoding("utf-8");
        Map<String, String> params = genParams(request);
        SearchService searchService = new SearchService();
        PrintWriter out = response.getWriter();
        logger.info("[ 查询时间：" + getNowTime() + " 方法：" + request.getMethod() + ", 参数：" + params.toString() + " ]");
        //        TODO:开始计时
        long startCalcTime = System.currentTimeMillis();
        //      op只有是elasticsearch_log_search时，才是查询的数据的接口
        if (!params.containsKey("op") || !params.get("op").equals("elasticsearch_log_search")) {
            return;
        }

        //      当这些字段为空时，参数极度不符合要求，返回code=-1
        else if (allEmpty(params)) {
            JSONObject nullObject = new JSONObject();
            nullObject.put("code", "-1");
            nullObject.put("data", "");
            out.print(nullObject);
            return;
        } else {
            //          TODO:优化.返回的数据放在二进制字节流中，而非字符流，是否可以提高很多性能
            try {
                JSONObject result = searchService.getESTotal(params.get("gid"),
                        params.get("startTime"), params.get("endTime"), params.get("dept"), params.get("sid"), params.get("userType"),
                        params.get("userValue"), params.get("type"), Integer.parseInt(params.get("patch")));

                //              TODO:打印查询耗费时间
                long endCalcTime = System.currentTimeMillis();
                long timeSub = endCalcTime - startCalcTime;
                logger.info("返回结果成功，共" + result.getJSONArray("data").size() + "条数据; 耗时：" + timeSub + "毫秒-------\n");
                out.println(result);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        out.close();
    }
    private Map<String, String> genParams(HttpServletRequest request) {
        Map<String, String> paramsMap = new HashMap<String, String>();
        paramsMap.put("op", request.getParameter("op"));
        paramsMap.put("type", request.getParameter("type"));
        paramsMap.put("startTime", request.getParameter("start_time"));
        paramsMap.put("endTime", request.getParameter("stop_time"));
        paramsMap.put("gid", request.getParameter("gid"));
        paramsMap.put("sid", request.getParameter("sid"));
        paramsMap.put("dept", request.getParameter("dept"));
        paramsMap.put("userType", request.getParameter("userType"));
        paramsMap.put("patch", request.getParameter("patch") == null ? "0" : String.valueOf(request.getParameter
                ("patch")));
        String userValue = request.getParameter("userValue");
        String decodeValue, encodeValue;
        try {
            logger.info("转码前：" + userValue);
            decodeValue = URLDecoder.decode(userValue, "utf-8");
            encodeValue = URLEncoder.encode(decodeValue, "utf-8");
            encodeValue = repStr(encodeValue);
            logger.info("中括号转码后：" + encodeValue);
            //          TODO:过滤，如果长度小于等于2,则直接返回提示“输入太短”
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
            encodeValue = request.getParameter("userValue");
        }
        //        paramsMap.put("userValue", encodeValue);
        paramsMap.put("userValue", request.getParameter("userValue")
                .replace("[","\\[")
                .replace("]","\\]"));
        return paramsMap;
    }
    //    将[1]南宫建柏替换为\[1\]南宫建柏
    private String repStr(String str) {
        return str.replaceAll("\\[", "\\\\[").replaceAll("\\]", "\\\\]");
    }

    private void setReqResp(HttpServletRequest request, HttpServletResponse response) {
        response.setContentType("text/json");
        response.setCharacterEncoding("utf-8");
        try {
            request.setCharacterEncoding("utf-8");
        } catch (UnsupportedEncodingException e) {
            //            TODO:打印转码失败日志，记录到error日志中
            e.printStackTrace();
        }
    }
    private Boolean allEmpty(Map<String, String> params) {
        //      TODO: 交流，验证是否是这些判断条件
        Boolean allEmptyFlag = params.get("gid") == null && params.get("sid") == null && params.get("dept") == null
                && params.get("userType") == null && params.get("userValue") == null;
        return allEmptyFlag;
    }

    private String getNowTime() {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");//日志时间格式
        String nowTime = sdf.format(new Date());
        return nowTime;
    }
}
