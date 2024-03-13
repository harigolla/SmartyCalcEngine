package com.smarty.calcengine.web.rest.calcengine;

import static com.smarty.calcengine.web.rest.calcengine.HedgeAlgoColumns.*;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.smarty.calcengine.service.api.dto.CalcEngineRequest;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;
import tech.tablesaw.api.ColumnType;
import tech.tablesaw.api.Table;
import tech.tablesaw.io.Destination;
import tech.tablesaw.io.json.JsonReadOptions;
import tech.tablesaw.io.json.JsonReader;
import tech.tablesaw.io.json.JsonWriter;

public class HedgeAlgoJson {

    public String calculate(CalcEngineRequest calcEngineRequest) throws JsonProcessingException {
        String jsonString = CalcEngineRequestUtil.getJson(calcEngineRequest);
        Map<String, ColumnType> maps = new HashMap<>();
        maps.put(QUANTITY, ColumnType.DOUBLE);
        maps.put(ALLOCATION_QTY, ColumnType.DOUBLE);
        maps.put(MARKET_VALUE, ColumnType.DOUBLE);
        maps.put(CTD, ColumnType.DOUBLE);
        maps.put(ORDER_ID, ColumnType.INTEGER);
        maps.put(RANK, ColumnType.INTEGER);
        maps.put(SIDE, ColumnType.STRING);
        maps.put(METRIC, ColumnType.STRING);

        JsonReadOptions jsonReadOptions = JsonReadOptions
            .builderFromString(jsonString)
            .path("/orders")
            .tableName("ORDER_DATA")
            .columnTypesPartial(maps)
            .build();

        JsonReader jsonReader = new JsonReader();
        Table table = jsonReader.read(jsonReadOptions);
        System.out.println(table);
        HedgeAlgo hedgeAlgo = new HedgeAlgo();
        Table returnTable = hedgeAlgo.calculate(table);
        JsonWriter jsonWriter = new JsonWriter();

        StringWriter sw = new StringWriter();
        Destination destination = new Destination(sw);
        jsonWriter.write(returnTable, destination);
        return sw.toString();
    }

    public static void main(String[] args) throws JsonProcessingException {
        HedgeAlgoJson hedgeAlgoJson = new HedgeAlgoJson();
        CalcEngineRequest calcEngineRequest = CalcEngineRequestUtil.getRequest(getJsonString());
        hedgeAlgoJson.calculate(calcEngineRequest);
    }

    private static String getJsonString() {
        return (
            "{\n" +
            "  \"orders\": [\n" +
            "    {\n" +
            "      \"orderId\": \"1\",\n" +
            "      \"side\": \"Buy\",\n" +
            "      \"rank\": 1,\n" +
            "      \"metric\": \"MV\",\n" +
            "      \"quantity\": 1000.0,\n" +
            "      \"allocQuantity\": 100.0,\n" +
            "      \"marketValue\": 10000.0,\n" +
            "      \"ctDuration\": 11.0\n" +
            "    },\n" +
            "    {\n" +
            "      \"orderId\": \"2\",\n" +
            "      \"side\": \"Buy\",\n" +
            "      \"rank\": 1,\n" +
            "      \"metric\": \"MV\",\n" +
            "      \"quantity\": 1000.0,\n" +
            "      \"allocQuantity\": 0.0,\n" +
            "      \"marketValue\": 10000.0,\n" +
            "      \"ctDuration\": 11.0\n" +
            "    },\n" +
            "    {\n" +
            "      \"orderId\": \"3\",\n" +
            "      \"side\": \"Buy\",\n" +
            "      \"rank\": 2,\n" +
            "      \"metric\": \"MV\",\n" +
            "      \"quantity\": 1000.0,\n" +
            "      \"allocQuantity\": 0.0,\n" +
            "      \"marketValue\": 10000.0,\n" +
            "      \"ctDuration\": 11.0\n" +
            "    },\n" +
            "    {\n" +
            "      \"orderId\": \"4\",\n" +
            "      \"side\": \"Buy\",\n" +
            "      \"rank\": 2,\n" +
            "      \"metric\": \"MV\",\n" +
            "      \"quantity\": 1000.0,\n" +
            "      \"allocQuantity\": 0.0,\n" +
            "      \"marketValue\": 10000.0,\n" +
            "      \"ctDuration\": 11.0\n" +
            "    },\n" +
            "    {\n" +
            "      \"orderId\": \"5\",\n" +
            "      \"side\": \"Sell\",\n" +
            "      \"rank\": 3,\n" +
            "      \"metric\": \"MV\",\n" +
            "      \"quantity\": 1000.0,\n" +
            "      \"allocQuantity\": 400.0,\n" +
            "      \"marketValue\": 10000.0,\n" +
            "      \"ctDuration\": 10.0\n" +
            "    },\n" +
            "    {\n" +
            "      \"orderId\": \"6\",\n" +
            "      \"side\": \"Sell\",\n" +
            "      \"rank\": 3,\n" +
            "      \"metric\": \"MV\",\n" +
            "      \"quantity\": 1000.0,\n" +
            "      \"allocQuantity\": 200.0,\n" +
            "      \"marketValue\": 10000.0,\n" +
            "      \"ctDuration\": 10.0\n" +
            "    },\n" +
            "    {\n" +
            "      \"orderId\": \"7\",\n" +
            "      \"side\": \"Sell\",\n" +
            "      \"rank\": 4,\n" +
            "      \"metric\": \"MV\",\n" +
            "      \"quantity\": 1000.0,\n" +
            "      \"allocQuantity\": 0.0,\n" +
            "      \"marketValue\": 10000.0,\n" +
            "      \"ctDuration\": 10.0\n" +
            "    },\n" +
            "    {\n" +
            "      \"orderId\": \"8\",\n" +
            "      \"side\": \"Sell\",\n" +
            "      \"rank\": 4,\n" +
            "      \"metric\": \"MV\",\n" +
            "      \"quantity\": 1000.0,\n" +
            "      \"allocQuantity\": 0.0,\n" +
            "      \"marketValue\": 10000.0,\n" +
            "      \"ctDuration\": 10.0\n" +
            "    },\n" +
            "    {\n" +
            "      \"orderId\": \"9\",\n" +
            "      \"side\": \"Sell\",\n" +
            "      \"rank\": 5,\n" +
            "      \"metric\": \"CTD\",\n" +
            "      \"quantity\": 500.0,\n" +
            "      \"allocQuantity\": 75.0,\n" +
            "      \"marketValue\": 0.0,\n" +
            "      \"ctDuration\": 4.0\n" +
            "    }\n" +
            "  ]\n" +
            "}"
        );
    }
}
