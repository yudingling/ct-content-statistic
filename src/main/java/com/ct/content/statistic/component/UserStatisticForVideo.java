package com.ct.content.statistic.component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;

import javax.annotation.PostConstruct;

import org.apache.commons.collections.CollectionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import com.ct.common.model.cache.UserStatisticForVideoCacheType;
import com.ct.common.model.mq.UserStatisticDataForVideo;
import com.ct.common.model.statistic.ItemUserStat;
import com.ct.common.model.statistic.VideoStat;
import com.ct.content.statistic.service.UserStatisticService;
import com.zeasn.common.cache.LocalCacheListener;
import com.zeasn.common.model.cache.LocalCacheAction;
import com.zeasn.common.util.Common;

@Component
public class UserStatisticForVideo extends LocalCacheListener<UserStatisticForVideoCacheType> {
	private final ConcurrentHashMap<Long, UserVideoStatObj> cache = new ConcurrentHashMap<>();
	private static final long LOCAL_EXPIRE_MS = 172800000; //expire in 2 days
	
	@Autowired
	private UserStatisticService statisticService;
	
	private Random rd = new Random();
	
	public UserStatisticForVideo() {
		super(UserStatisticForVideoCacheType.class);
	}
	
	@PostConstruct
	private void init(){
		new Thread(this::clearExpire).start();
	}
	
	private void clearExpire(){
		Common.sleepWithInterrupt(rd.nextInt((int) 360000));
		
		while(!Thread.currentThread().isInterrupted()){
			if(!this.cache.isEmpty()){
				long cmpTs = System.currentTimeMillis() - LOCAL_EXPIRE_MS;
				
				this.cache.entrySet().parallelStream().forEach(item -> {
					if(cmpTs > item.getValue().getTs()){
						this.cache.remove(item.getKey());
					}
				});
			}
			
			Common.sleepWithInterrupt(LOCAL_EXPIRE_MS);
		}
	}
	
	private UserVideoStatObj getStatObj(long uId) {
		UserVideoStatObj statObj = this.cache.get(uId);
		if(statObj == null) {
			statObj = this.getStatObjFromDb(uId);
		}
		
		statObj.refreshTs();
		
		return statObj;
	}
	
	private UserVideoStatObj getStatObjFromDb(long uId) {
		Lock writeLock = this.lock.writeLock();
		try{
			writeLock.lock();
			
			//double check
			UserVideoStatObj statObj = this.cache.get(uId);
			if(statObj != null) {
				return statObj;
			}
			
			statObj = new UserVideoStatObj(
					this.createMap(this.statisticService.getAllLikedItems(uId)), 
					this.createMap(this.statisticService.getAllCollectedItems(uId)), 
					this.createMap(this.statisticService.getAllSubedUsers(uId)));
			
			UserVideoStatObj pre = this.cache.putIfAbsent(uId, statObj);
			if(pre != null) {
				statObj = pre;
			}
			
			return statObj;
			
		}finally{
			writeLock.unlock();
		}
	}
	
	private ConcurrentHashMap<Long, Byte> createMap(List<Long> list) {
		ConcurrentHashMap<Long, Byte> result = new ConcurrentHashMap<>();
		
		if(CollectionUtils.isNotEmpty(list)) {
			list.forEach(tmp -> {
				result.put(tmp, (byte)1);
			});
		}
		
		return result;
	}
	
	@Override
	public void onChanged(LocalCacheAction cacheAction, Object msg) {
		UserStatisticDataForVideo data = this.getValidData(msg);
		
		if(data != null) {
			UserVideoStatObj obj = this.getStatObj(data.getTokenUid());
			
			switch(data.getStype()) {
				case LIKE_VIDEO:
					obj.like(data.getItemId());
					break;
					
				case UNLIKE_VIDEO:
					obj.unlike(data.getItemId());
					break;
					
				case COLLECT_VIDEO:
					obj.collect(data.getItemId());
					break;
					
				case UNCOLLECT_VIDEO:
					obj.uncollect(data.getItemId());
					break;
					
				case SUBSCRIBE_USER:
					obj.sub(data.getuId());
					break;
					
				case UNSUBSCRIBE_USER:
					obj.unsub(data.getuId());
					break;
				
				default:
					break;
			}
		}
	}
	
	private UserStatisticDataForVideo getValidData(Object msg){
		return msg instanceof UserStatisticDataForVideo ? (UserStatisticDataForVideo) msg : null;
	}
	
	public VideoStat getUserVideoStat(long uId, Set<Long> itemIdsForStat, Set<Long> uIdsForStat) {
		UserVideoStatObj obj = this.getStatObj(uId);
		
		Map<Long, ItemUserStat> itemUser = new HashMap<>();
		Map<Long, Boolean> followed = new HashMap<>();
		
		if(CollectionUtils.isNotEmpty(itemIdsForStat)) {
			itemIdsForStat.forEach(itemId -> {
				itemUser.put(itemId, new ItemUserStat(obj.isLiked(itemId), obj.isCollected(itemId)));
			});
		}
		
		if(CollectionUtils.isNotEmpty(uIdsForStat)) {
			uIdsForStat.forEach(tmpUid -> {
				followed.put(tmpUid, obj.isSubed(tmpUid));
			});
		}
		
		return new VideoStat(itemUser, followed);
	}
	
	public class UserVideoStatObj{
		private ConcurrentHashMap<Long, Byte> likeMap;
		private ConcurrentHashMap<Long, Byte> collectMap;
		private ConcurrentHashMap<Long, Byte> subMap;
		private Long ts;
		
		public void like(long itemId) {
			this.likeMap.put(itemId, (byte)1);
		}
		public void unlike(long itemId) {
			this.likeMap.remove(itemId);
		}
		public boolean isLiked(long itemId) {
			return this.likeMap.containsKey(itemId);
		}
		
		
		public void collect(long itemId) {
			this.collectMap.put(itemId, (byte)1);
		}
		public void uncollect(long itemId) {
			this.collectMap.remove(itemId);
		}
		public boolean isCollected(long itemId) {
			return this.collectMap.containsKey(itemId);
		}
		
		public void sub(long uId) {
			this.subMap.put(uId, (byte)1);
		}
		public void unsub(long uId) {
			this.subMap.remove(uId);
		}
		public boolean isSubed(long uId) {
			return this.subMap.containsKey(uId);
		}
		
		public Long getTs() {
			return ts;
		}
		
		public void refreshTs () {
			this.ts = System.currentTimeMillis();
		}
		
		public UserVideoStatObj(ConcurrentHashMap<Long, Byte> likeMap, ConcurrentHashMap<Long, Byte> collectMap,
				ConcurrentHashMap<Long, Byte> subMap) {
			super();
			
			this.likeMap = likeMap;
			this.collectMap = collectMap;
			this.subMap = subMap;
			
			this.ts = System.currentTimeMillis() + rd.nextInt(14400000);
		}
	}
}
