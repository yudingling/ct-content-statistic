package com.ct.content.statistic.model;

import com.ct.common.model.ProtectAction;

public class ItemStatProtectObj extends ProtectObj {
	private static final long serialVersionUID = 956640767044541999L;
	
	private Long itemId;
	private Long incView;
	private Long incLike;

	public Long getItemId() {
		return itemId;
	}

	public void setItemId(Long itemId) {
		this.itemId = itemId;
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

	public ItemStatProtectObj(ProtectAction action, Long itemId, Long incView, Long incLike) {
		super(action);
		
		this.itemId = itemId;
		this.incView = incView;
		this.incLike = incLike;
	}
}
