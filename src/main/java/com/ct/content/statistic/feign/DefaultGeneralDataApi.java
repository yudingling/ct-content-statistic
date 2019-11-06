package com.ct.content.statistic.feign;

import org.springframework.cloud.openfeign.FeignClient;

import com.zeasn.common.feign.api.GeneralDataApi;

@FeignClient(value = "general-data")
public interface DefaultGeneralDataApi extends GeneralDataApi {
}
