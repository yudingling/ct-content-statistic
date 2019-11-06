package com.ct.content.statistic.service;

import java.util.Map;
import java.util.Set;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.ct.common.mapper.ZLauncherGroupFkRelMapper;
import com.ct.common.mapper.ZLauncherItemStatisticMapper;
import com.ct.common.model.ProtectAction;
import com.ct.common.model.db.ZLauncherItemStatistic;
import com.ct.content.statistic.model.ItemStatProtectObj;
import com.zeasn.common.daemon.Daemon;
import com.zeasn.common.daemon.IWriteBack;
import com.zeasn.common.util.DbException;

@Service
public class ContentStatisticService implements IWriteBack<ItemStatProtectObj> {
	@Autowired
	private ZLauncherItemStatisticMapper itemStatMapper;
	@Autowired
	private ZLauncherGroupFkRelMapper launcherGroupFkRelMapper;
	@Autowired
	private Daemon daemon;
	
	public void incStatistic(Long itemId, Long incView, Long incLike){
		//itemViewCount in GROUPFKREL will be removed in the future.
		//  itemViewCount in ES item has not been used yet.
		
		//this.daemon.protectDb(this, new ItemStatProtectObj(ProtectAction.ITEM_GROUPFKREL, itemId, incView, incLike), -1, this.launcherGroupFkRelMapper.getClass().getName());
		
		this.daemon.protectDb(this, new ItemStatProtectObj(ProtectAction.ITEM_STATISTIC, itemId, incView, incLike), -1, this.itemStatMapper.getClass().getName());
	}
	
	public Map<Long, ZLauncherItemStatistic> getStatistic(Set<Long> itemIds){
		return this.itemStatMapper.getItemsStatistic(itemIds);
	}
	
	public ZLauncherItemStatistic getStatistic(Long itemId){
		return this.itemStatMapper.selectByPrimaryKey(itemId);
	}

	@Override
	public boolean writeBack(ItemStatProtectObj data) {
		switch(data.getAction()){
			case ITEM_GROUPFKREL:
				return this.updateGroupFkRels(data.getItemId(), data.getIncView());
				
			case ITEM_STATISTIC:
				return this.updateItem(data.getItemId(), data.getIncView(), data.getIncLike());
				
			default:
				return true;
		}
	}
	
	private boolean updateGroupFkRels(Long itemId, Long incView){
		if(incView != 0){
			this.launcherGroupFkRelMapper.updateViewCount(itemId, incView);
		}
		
		return true;
	}
	
	private boolean updateItem(Long itemId, Long incView, Long incLike){
		if(incView != 0 || incLike != 0){
			if(this.itemStatMapper.updateItemStatistic(itemId, incView, incLike) == 0){
				boolean retry = false;
				
				try{
					ZLauncherItemStatistic stat = new ZLauncherItemStatistic(itemId, incView, incLike);
					if(this.itemStatMapper.insert(stat) != 1){
						retry = true;
					}
					
				}catch(Exception e){
					if(DbException.isDuplicateException(e)){
						retry = true;
						
					}else{
						throw e;
					}
				}
				
				if(retry){
					this.itemStatMapper.updateItemStatistic(itemId, incView, incLike);
				}
			}
		}
		
		return true;
	}
}
