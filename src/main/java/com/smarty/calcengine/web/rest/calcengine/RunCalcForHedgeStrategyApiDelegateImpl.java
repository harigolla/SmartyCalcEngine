package com.smarty.calcengine.web.rest.calcengine;

import com.smarty.calcengine.service.api.dto.CalcEngineRequest;
import com.smarty.calcengine.service.api.dto.CalcEngineResponse;
import com.smarty.calcengine.web.api.ApiUtil;
import com.smarty.calcengine.web.api.RunCalcForHedgeStrategyApiDelegate;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;

@Component
public class RunCalcForHedgeStrategyApiDelegateImpl implements RunCalcForHedgeStrategyApiDelegate {

    @Override
    public ResponseEntity<CalcEngineResponse> runCalcForHedgeStrategy(CalcEngineRequest calcEngineRequest) {
        getRequest()
            .ifPresent(request -> {
                for (MediaType mediaType : MediaType.parseMediaTypes(request.getHeader("Accept"))) {
                    if (mediaType.isCompatibleWith(MediaType.valueOf("application/json"))) {
                        String exampleString = "{ \"message\" : \"message\" }";
                        ApiUtil.setExampleResponse(request, "application/json", exampleString);
                        break;
                    }
                }
            });

        CalcEngineResponse calcEngineResponse = new CalcEngineResponse();

        try {
            HedgeAlgoJson hedgeAlgoJson = new HedgeAlgoJson();
            String response = hedgeAlgoJson.calculate(calcEngineRequest);
            calcEngineResponse.setMessage(response);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return ResponseEntity.ok().header("responseType", "CalcEngineResponse").body(calcEngineResponse);
    }
}
