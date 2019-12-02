package com.ct.content.statistic.controller;

import java.util.Map;
import java.util.Set;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.ct.common.model.statistic.UserStatisticObj;
import com.ct.common.model.statistic.VideoStat;
import com.ct.content.statistic.component.UserStatistic;
import com.ct.content.statistic.component.UserStatisticForVideo;

@RestController
@RequestMapping("/userStat")
public class UserStatisticController {
	@Autowired
	private UserStatistic  stat;
	@Autowired
	private UserStatisticForVideo statVideo;
	
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
	
	@RequestMapping(value = "/videoStat", method = RequestMethod.POST)
	public VideoStat getVideoStat(
			@RequestParam long uId, 
			@RequestParam(value = "itemIdsForStat[]") Set<Long> itemIdsForStat, 
			@RequestParam(value = "uIdsForStat[]") Set<Long> uIdsForStat) {
		
		return this.statVideo.getUserVideoStat(uId, itemIdsForStat, uIdsForStat);
	}
}
