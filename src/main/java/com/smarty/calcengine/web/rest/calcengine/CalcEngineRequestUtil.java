package com.smarty.calcengine.web.rest.calcengine;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.smarty.calcengine.service.api.dto.CalcEngineRequest;

public class CalcEngineRequestUtil {

    public static String getJson(CalcEngineRequest calcEngineRequest) throws JsonProcessingException {
        ObjectMapper objectMapper = new ObjectMapper();
        return objectMapper.writeValueAsString(calcEngineRequest);
    }

    public static CalcEngineRequest getRequest(String jsonString) throws JsonProcessingException {
        ObjectMapper objectMapper = new ObjectMapper();
        return objectMapper.readValue(jsonString, CalcEngineRequest.class);
    }

    public static void main(String[] args) throws JsonProcessingException {
        String jsonString =
            "{\n" +
            "  \"orders\": [\n" +
            "    {\n" +
            "      \"orderId\": \"1\",\n" +
            "      \"side\": \"Buy\",\n" +
            "      \"rank\": 1,\n" +
            "      \"metric\": \"MV\",\n" +
            "      \"quantity\": 1000,\n" +
            "      \"allocQuantity\": 100,\n" +
            "      \"marketValue\": 10000,\n" +
            "      \"ctDuration\": 11\n" +
            "    },\n" +
            "    {\n" +
            "      \"orderId\": \"2\",\n" +
            "      \"side\": \"Buy\",\n" +
            "      \"rank\": 1,\n" +
            "      \"metric\": \"MV\",\n" +
            "      \"quantity\": 1000,\n" +
            "      \"allocQuantity\": 0,\n" +
            "      \"marketValue\": 10000,\n" +
            "      \"ctDuration\": 11\n" +
            "    },\n" +
            "    {\n" +
            "      \"orderId\": \"3\",\n" +
            "      \"side\": \"Buy\",\n" +
            "      \"rank\": 2,\n" +
            "      \"metric\": \"MV\",\n" +
            "      \"quantity\": 1000,\n" +
            "      \"allocQuantity\": 0,\n" +
            "      \"marketValue\": 10000,\n" +
            "      \"ctDuration\": 11\n" +
            "    },\n" +
            "    {\n" +
            "      \"orderId\": \"4\",\n" +
            "      \"side\": \"Buy\",\n" +
            "      \"rank\": 2,\n" +
            "      \"metric\": \"MV\",\n" +
            "      \"quantity\": 1000,\n" +
            "      \"allocQuantity\": 0,\n" +
            "      \"marketValue\": 10000,\n" +
            "      \"ctDuration\": 11\n" +
            "    },\n" +
            "    {\n" +
            "      \"orderId\": \"5\",\n" +
            "      \"side\": \"Sell\",\n" +
            "      \"rank\": 3,\n" +
            "      \"metric\": \"MV\",\n" +
            "      \"quantity\": 1000,\n" +
            "      \"allocQuantity\": 400,\n" +
            "      \"marketValue\": 10000,\n" +
            "      \"ctDuration\": 10\n" +
            "    },\n" +
            "    {\n" +
            "      \"orderId\": \"6\",\n" +
            "      \"side\": \"Sell\",\n" +
            "      \"rank\": 3,\n" +
            "      \"metric\": \"MV\",\n" +
            "      \"quantity\": 1000,\n" +
            "      \"allocQuantity\": 200,\n" +
            "      \"marketValue\": 10000,\n" +
            "      \"ctDuration\": 10\n" +
            "    },\n" +
            "    {\n" +
            "      \"orderId\": \"7\",\n" +
            "      \"side\": \"Sell\",\n" +
            "      \"rank\": 4,\n" +
            "      \"metric\": \"MV\",\n" +
            "      \"quantity\": 1000,\n" +
            "      \"allocQuantity\": 0,\n" +
            "      \"marketValue\": 10000,\n" +
            "      \"ctDuration\": 10\n" +
            "    },\n" +
            "    {\n" +
            "      \"orderId\": \"8\",\n" +
            "      \"side\": \"Sell\",\n" +
            "      \"rank\": 4,\n" +
            "      \"metric\": \"MV\",\n" +
            "      \"quantity\": 1000,\n" +
            "      \"allocQuantity\": 0,\n" +
            "      \"marketValue\": 10000,\n" +
            "      \"ctDuration\": 10\n" +
            "    },\n" +
            "    {\n" +
            "      \"orderId\": \"9\",\n" +
            "      \"side\": \"Sell\",\n" +
            "      \"rank\": 5,\n" +
            "      \"metric\": \"CTD\",\n" +
            "      \"quantity\": 500,\n" +
            "      \"allocQuantity\": 75,\n" +
            "      \"marketValue\": 0,\n" +
            "      \"ctDuration\": 4\n" +
            "    }\n" +
            "  ]\n" +
            "}";

        CalcEngineRequest calcEngineRequest = CalcEngineRequestUtil.getRequest(jsonString);

        System.out.printf(CalcEngineRequestUtil.getJson(calcEngineRequest));
    }
}
