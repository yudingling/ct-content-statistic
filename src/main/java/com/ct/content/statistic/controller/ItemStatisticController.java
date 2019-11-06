package com.ct.content.statistic.controller;

import java.util.Map;
import java.util.Set;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.ct.common.model.statistic.ItemStatisticObj;
import com.ct.content.statistic.component.ContentStatistic;
import com.ct.content.statistic.component.UserStatistic;

@RestController
@RequestMapping("/itemStat")
public class ItemStatisticController {
	@Autowired
	private ContentStatistic  itemStat;
	@Autowired
	private UserStatistic  userStat;
	
	@RequestMapping(value = "/like", method = RequestMethod.POST)
	public void incLike(@RequestParam long itemId, @RequestParam long uId, @RequestParam int size) {
		this.itemStat.incLike(itemId, size);
		
		this.userStat.incLike(uId, size);
	}
	
	@RequestMapping(value = "/view", method = RequestMethod.POST)
	public void incView(@RequestParam long itemId, @RequestParam long uId, @RequestParam int size) {
		this.itemStat.incView(itemId, size);
		
		this.userStat.incView(uId, size);
	}
	
	@RequestMapping(method = RequestMethod.GET)
	public ItemStatisticObj getStat(@RequestParam long itemId) {
		return this.itemStat.getItemStatistic(itemId);
	}
	
	@RequestMapping(value = "/batch", method = RequestMethod.GET)
	public Map<Long, ItemStatisticObj> getStatList(@RequestParam(value = "itemIds[]") Set<Long> itemIds) {
		return this.itemStat.getItemStatistic(itemIds);
	}
}
