package com.ct.content.statistic.controller;

import java.util.Map;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import com.ct.common.model.statistic.TopicStatisticObj;
import com.ct.content.statistic.component.TopicStatistic;

@RestController
@RequestMapping("/topicStat")
public class TopicStatisticController {
	@Autowired
	private TopicStatistic  topicStat;
	
	@RequestMapping(value = "/like", method = RequestMethod.POST)
	public void incLike(@RequestParam(value = "topicIds[]") Set<Long> topicIds, @RequestParam int size) {
		if(CollectionUtils.isNotEmpty(topicIds)) {
			topicIds.forEach(topicId -> {
				this.topicStat.incLike(topicId, size);
			});
		}
	}
	
	@RequestMapping(value = "/view", method = RequestMethod.POST)
	public void incView(@RequestParam(value = "topicIds[]") Set<Long> topicIds, @RequestParam int size) {
		if(CollectionUtils.isNotEmpty(topicIds)) {
			topicIds.forEach(topicId -> {
				this.topicStat.incView(topicId, size);
			});
		}
	}
	
	@RequestMapping(method = RequestMethod.GET)
	public TopicStatisticObj getStat(@RequestParam long topicId) {
		return this.topicStat.getTopicStatistic(topicId);
	}
	
	@RequestMapping(value = "/batch", method = RequestMethod.GET)
	public Map<Long, TopicStatisticObj> getStatList(@RequestParam(value = "topicIds[]") Set<Long> topicIds) {
		return this.topicStat.getTopicStatistic(topicIds);
	}
}
