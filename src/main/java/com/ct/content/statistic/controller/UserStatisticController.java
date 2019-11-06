package com.ct.content.statistic.controller;

import java.util.Map;
import java.util.Set;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.ct.common.model.statistic.UserStatisticObj;
import com.ct.content.statistic.component.UserStatistic;

@RestController
@RequestMapping("/userStat")
public class UserStatisticController {
	@Autowired
	private UserStatistic  stat;
	
	@RequestMapping(value = "/sub", method = RequestMethod.POST)
	public void incSub(@RequestParam long uId, @RequestParam int size) {
		this.stat.incSubscribe(uId, size);
	}
	
	@RequestMapping(method = RequestMethod.GET)
	public UserStatisticObj getStat(@RequestParam long uId) {
		return this.stat.getUserStatistic(uId);
	}
	
	@RequestMapping(value = "/batch", method = RequestMethod.GET)
	public Map<Long, UserStatisticObj> getStatList(@RequestParam(value = "uIds[]") Set<Long> uIds) {
		return this.stat.getUserStatistic(uIds);
	}
}
