package com.ct.content.statistic.service;

import java.util.Map;
import java.util.Set;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import com.ct.common.mapper.ZTopicStatisticMapper;
import com.ct.common.model.ProtectAction;
import com.ct.common.model.db.ZTopicStatistic;
import com.ct.content.statistic.model.TopicStatProtectObj;
import com.zeasn.common.daemon.Daemon;
import com.zeasn.common.daemon.IWriteBack;

@Service
public class TopicStatisticService implements IWriteBack<TopicStatProtectObj> {
	@Autowired
	private ZTopicStatisticMapper topicStatMapper;
	@Autowired
	private Daemon daemon;
	
	public void incStatistic(Long topicId, Long incView, Long incLike){
		this.daemon.protectDb(this, new TopicStatProtectObj(ProtectAction.TOPIC_STATISTIC, topicId, incView, incLike), -1, this.topicStatMapper.getClass().getName());
	}

	@Override
	public boolean writeBack(TopicStatProtectObj data) {
		switch(data.getAction()){
			case TOPIC_STATISTIC:
				return this.updateItem(data.getTopicId(), data.getIncView(), data.getIncLike());
				
			default:
				return true;
		}
	}
	
	public Map<Long, ZTopicStatistic> getStatistic(Set<Long> topicIds){
		return this.topicStatMapper.getStatistic(topicIds);
	}
	
	public ZTopicStatistic getStatistic(long topicId){
		return this.topicStatMapper.selectByPrimaryKey(topicId);
	}
	
	private boolean updateItem(Long topicId, Long incView, Long incLike){
		if(incView != 0 || incLike != 0){
			this.topicStatMapper.updateViewAndLike(topicId, incView, incLike);
		}
		
		return true;
	}
}
