package com.ct.content.statistic.service;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.ct.common.mapper.ZUserCollectMapper;
import com.ct.common.mapper.ZUserLikeMapper;
import com.ct.common.mapper.ZUserStatisticMapper;
import com.ct.common.mapper.ZUserSubscribeMapper;
import com.ct.common.model.ProtectAction;
import com.ct.common.model.db.ZUserStatistic;
import com.ct.content.statistic.model.UserStatProtectObj;
import com.zeasn.common.daemon.Daemon;
import com.zeasn.common.daemon.IWriteBack;
import com.zeasn.common.util.DbException;

@Service
public class UserStatisticService implements IWriteBack<UserStatProtectObj> {
	@Autowired
	private ZUserStatisticMapper userStatMapper;
	@Autowired
	private ZUserLikeMapper likedMapper;
	@Autowired
	private ZUserCollectMapper collectedMapper;
	@Autowired
	private ZUserSubscribeMapper subedMapper;
	
	@Autowired
	private Daemon daemon;
	
	public List<Long> getAllLikedItems(long uId){
		return this.likedMapper.getAllLikedItems(uId);
	}
	
	public List<Long> getAllCollectedItems(long uId){
		return this.collectedMapper.getAllCollectedItems(uId);
	}
	
	public List<Long> getAllSubedUsers(long uId){
		return this.subedMapper.getUsers(uId);
	}
	
	public Map<Long, ZUserStatistic> getStatistic(Set<Long> uIds){
		return this.userStatMapper.getUsersStatistic(uIds);
	}
	
	public ZUserStatistic getStatistic(Long uId){
		return this.userStatMapper.selectByPrimaryKey(uId);
	}
	
	public void incStatistic(Long uId, Long incView, Long incLike, Long incSub, Long incPost){
		this.daemon.protectDb(this, new UserStatProtectObj(ProtectAction.USER_STATISTIC, uId, incView, incLike, incSub, incPost), -1, this.userStatMapper.getClass().getName());
	}

	@Override
	public boolean writeBack(UserStatProtectObj data) {
		switch(data.getAction()){
			case USER_STATISTIC:
				return this.updateUserStatistic(data);
				
			default:
				return true;
		}
	}
	
	private boolean updateUserStatistic(UserStatProtectObj data) {
		if(data.getIncLike() != 0 || data.getIncPost() != 0 || data.getIncSub() != 0 || data.getIncView() != 0) {
			if(this.userStatMapper.updateUserStatistic(data.getuId(), data.getIncView(), data.getIncLike(), data.getIncSub(), data.getIncPost()) == 0){
				boolean retry = false;
				
				try{
					ZUserStatistic stat = new ZUserStatistic(data.getuId(), data.getIncSub(), data.getIncView(), data.getIncLike(), data.getIncPost());
					if(this.userStatMapper.insert(stat) != 1){
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
					this.userStatMapper.updateUserStatistic(data.getuId(), data.getIncView(), data.getIncLike(), data.getIncSub(), data.getIncPost());
				}
			}
		}
		
		return true;
	}
}
