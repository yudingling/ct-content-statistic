package com.ct.content.statistic.model;

import com.ct.common.model.ProtectAction;

public class TopicStatProtectObj extends ProtectObj {
	private static final long serialVersionUID = -8814158256402502720L;
	
	private Long topicId;
	private Long incView;
	private Long incLike;

	public Long getTopicId() {
		return topicId;
	}

	public void setTopicId(Long topicId) {
		this.topicId = topicId;
	}

	public Long getIncView() {
		return incView;
	}

	public void setIncView(Long incView) {
		this.incView = incView;
	}

	public Long getIncLike() {
		return incLike;
	}

	public void setIncLike(Long incLike) {
		this.incLike = incLike;
	}

	public TopicStatProtectObj(ProtectAction action, Long topicId, Long incView, Long incLike) {
		super(action);
		
		this.topicId = topicId;
		this.incView = incView;
		this.incLike = incLike;
	}
}
