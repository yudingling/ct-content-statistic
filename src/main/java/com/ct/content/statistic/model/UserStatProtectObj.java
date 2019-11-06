package com.ct.content.statistic.model;

import com.ct.common.model.ProtectAction;

public class UserStatProtectObj extends ProtectObj {
	private static final long serialVersionUID = -8735164979032933130L;
	
	private Long uId;
	private Long incView;
	private Long incLike;
	private Long incSub;
	private Long incPost;

	public Long getuId() {
		return uId;
	}

	public void setuId(Long uId) {
		this.uId = uId;
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

	public Long getIncSub() {
		return incSub;
	}

	public void setIncSub(Long incSub) {
		this.incSub = incSub;
	}

	public Long getIncPost() {
		return incPost;
	}

	public void setIncPost(Long incPost) {
		this.incPost = incPost;
	}

	public UserStatProtectObj(ProtectAction action, Long uId, Long incView, Long incLike, Long incSub, Long incPost) {
		super(action);
		
		this.uId = uId;
		this.incView = incView;
		this.incLike = incLike;
		this.incSub = incSub;
		this.incPost = incPost;
	}
}
