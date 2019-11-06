package com.ct.content.statistic.model;

import java.io.Serializable;

import com.ct.common.model.ProtectAction;

public abstract class ProtectObj implements Serializable {
	private static final long serialVersionUID = -7815864361485360730L;
	
	private ProtectAction action;
	
	public ProtectAction getAction() {
		return action;
	}

	public void setAction(ProtectAction action) {
		this.action = action;
	}
	
	public ProtectObj(ProtectAction action) {
		super();
		this.action = action;
	}
}
