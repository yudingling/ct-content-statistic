package com.ct.content.statistic.component;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.PostConstruct;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.ct.common.model.cache.UserStatisticCacheType;
import com.ct.common.model.db.ZUserStatistic;
import com.ct.common.model.mq.UserStatisticData;
import com.ct.common.model.statistic.UserStatisticObj;
import com.ct.content.statistic.service.UserStatisticService;
import com.zeasn.common.cache.LocalCacheListener;
import com.zeasn.common.log.MyLog;
import com.zeasn.common.log.RuntimeLog;
import com.zeasn.common.model.cache.LocalCacheAction;
import com.zeasn.common.util.Common;

import io.netty.util.internal.ConcurrentSet;

@Component
public class UserStatistic extends LocalCacheListener<UserStatisticCacheType> {
	private static final MyLog log = MyLog.getLog(UserStatistic.class);
	
	private final ConcurrentHashMap<Long, UserStatObj> cache = new ConcurrentHashMap<>();
	private static final long LOCAL_EXPIRE_MS = 3600000; //expire in 60 mins
	private static final long UPDATE_DURATION_MS = 300000; //update in 5 min
	
	@Autowired
	private UserStatisticService statisticService;
	
	private Random rd = new Random();
	
	public UserStatistic() {
		super(UserStatisticCacheType.class);
	}
	
	@PostConstruct
	private void init(){
		new Thread(this::clearExpire).start();
		new Thread(this::updateInner).start();
	}
	
	private void clearExpire(){
		Common.sleepWithInterrupt(rd.nextInt((int) 360000));
		
		while(!Thread.currentThread().isInterrupted()){
			if(!this.cache.isEmpty()){
				long cmpTs = System.currentTimeMillis() - LOCAL_EXPIRE_MS * 24;
				
				this.cache.entrySet().parallelStream().forEach(item -> {
					UserStatObj stat = item.getValue();
					
					boolean canRemove = Math.abs(stat.getIncLike()) == 0 && Math.abs(stat.getIncView()) == 0 && Math.abs(stat.getIncSub()) == 0;
					
					if(canRemove && cmpTs > item.getValue().getTs()){
						this.cache.remove(item.getKey());
					}
				});
			}
			
			//check every 1 hour
			Common.sleepWithInterrupt(LOCAL_EXPIRE_MS);
		}
	}
	
	private void updateInner(){
		Common.sleepWithInterrupt(rd.nextInt((int) 36000));
		
		while(!Thread.currentThread().isInterrupted()){
			if(!this.cache.isEmpty()){
				Set<Long> idsForReset = new ConcurrentSet<>();
				
				this.cache.entrySet().parallelStream().forEach(item -> {
					int uptThreshold = 10 + rd.nextInt(100);
					
					Long uptId = this.incStatistic(item.getKey(), item.getValue(), uptThreshold);
					if(uptId != null){
						idsForReset.add(uptId);
					}
				});
				
				if(CollectionUtils.isNotEmpty(idsForReset)){
					this.resetLikeAndView(idsForReset);
				}
			}
			
			Common.sleepWithInterrupt(UPDATE_DURATION_MS);
		}
	}
	
	/**
	 * return the uId which need to reset from db
	 */
	private Long incStatistic(Long uId, UserStatObj stat, int uptThreshold){
		if(Math.abs(stat.getIncView()) >= uptThreshold || Math.abs(stat.getIncLike()) >= uptThreshold 
				|| (System.currentTimeMillis() - stat.getLastUpdateTs() > LOCAL_EXPIRE_MS)){
			
			Long incView = stat.resetIncView();
			Long incLike = stat.resetIncLike();
			Long incSub = stat.resetIncSub();
			
			if(incView != 0 || incLike != 0 || incSub != 0){
				this.statisticService.incStatistic(uId, incView, incLike, incSub, 0l);
			}
			
			return uId;
			
		}else{
			return null;
		}
	}
	
	private void resetLikeAndView(Set<Long> idsForReset){
		Set<Long> idList = new HashSet<>();
		for(Long itemId : idsForReset){
			if(idList.size() > 50){
				this.resetLikeAndViewInBatch(idList);
			}
			
			idList.add(itemId);
		}
		
		if(CollectionUtils.isNotEmpty(idList)){
			this.resetLikeAndViewInBatch(idList);
		}
	}
	
	private void resetLikeAndViewInBatch(Set<Long> uIds){
		try{
			Map<Long, ZUserStatistic> stats = this.statisticService.getStatistic(uIds);
			if(MapUtils.isNotEmpty(stats)){
				stats.forEach((uId, uStat) -> {
					UserStatObj statObj = this.cache.get(uId);
					if(statObj != null){
						statObj.resetStat(uStat);
					}
				});
			}
			
		}catch(Exception ex){
			log.error("update user statistic from db error: " + ex.getMessage(), ex);
		}
		
		uIds.clear();
	}
	
	private void delData(UserStatisticData data){
		this.cache.remove(data.getuId());
	}
	
	@Override
	public void onChanged(LocalCacheAction cacheAction, Object msg) {
		UserStatisticData data = this.getValidData(msg);
		switch(cacheAction){
			case DELETE:
				if(data != null){
					this.delData(data);
				}
				break;
			
			case UPDATE:
			case ADD:
			case RELOAD:
				log.warn(RuntimeLog.build(data, "UserStatistic doesn't support add/update/reload action."));
				break;
				
			default:
				break;
		}
	}
	
	private UserStatisticData getValidData(Object msg){
		return msg instanceof UserStatisticData ? (UserStatisticData) msg : null;
	}
	
	public void incSubscribe(long uId, int size) {
		UserStatObj obj = this.getStat(uId);
		
		obj.incSubscribe(size);
	}
	
	public void incView(long uId, int size) {
		UserStatObj obj = this.getStat(uId);
		
		obj.incView(size);
	}
	
	public void incLike(long uId, int size) {
		UserStatObj obj = this.getStat(uId);
		
		obj.incLike(size);
	}
	
	private UserStatObj getStat(long uId) {
		UserStatObj obj = this.cache.get(uId);
		
		if(obj == null){
			obj = this.queryFromDB(uId);			
		}
		
		return obj;
	}
	
	public UserStatisticObj getUserStatistic(long uId) {
		UserStatObj obj = this.cache.get(uId);
		
		if(obj == null){
			obj = this.queryFromDB(uId);			
		}
		
		return obj.getObj();
	}
	
	public Map<Long, UserStatisticObj> getUserStatistic(Set<Long> uIds){
		Set<Long> idsForDB = new HashSet<>();
		Map<Long, UserStatisticObj> map = new HashMap<>();
		
		uIds.forEach(id -> {
			UserStatObj obj = this.cache.get(id);
			if(obj != null){
				map.put(id, obj.getObj());
				
			}else{
				idsForDB.add(id);
			}
		});
		
		if(!idsForDB.isEmpty()){
			this.queryFromDB(idsForDB, map);
		}
		
		return map;
	}
	
	private void queryFromDB(Set<Long> idsForDB, Map<Long, UserStatisticObj> map){
		Map<Long, ZUserStatistic> statMap = this.statisticService.getStatistic(idsForDB);
		
		idsForDB.forEach(uId -> {
			ZUserStatistic stat = statMap.get(uId);
			UserStatObj ss = stat != null ? new UserStatObj(stat) : new UserStatObj();
			
			map.put(uId, ss.getObj());
			this.cache.put(uId, ss);
		});
	}
	
	private UserStatObj queryFromDB(Long idForDB){
		ZUserStatistic stat = this.statisticService.getStatistic(idForDB);
		
		UserStatObj ss = stat != null ? new UserStatObj(stat) : new UserStatObj();
		
		this.cache.put(idForDB, ss);
		return ss;
	}
	
	public class UserStatObj{
		private AtomicLong subed;
		private AtomicLong viewed;
		private AtomicLong liked;
		private AtomicInteger posted;
		
		private AtomicLong incSub = new AtomicLong();
		private AtomicLong incView = new AtomicLong();
		private AtomicLong incLike = new AtomicLong();
		
		private Long ts;
		private Long lastUpdateTs;
		
		public long getSubed() {
			long val = subed.get();
			return val >= 0 ? val : 0;
		}

		public long getViewed() {
			long val = viewed.get();
			return val >= 0 ? val : 0;
		}

		public long getLiked() {
			long val = liked.get();
			return val >= 0 ? val : 0;
		}

		public int getPosted() {
			int val = posted.get();
			return val >= 0 ? val : 0;
		}

		public Long getTs() {
			return ts;
		}
		
		public void incSubscribe(int size) {
			this.subed.addAndGet(size);
			
			this.incSub.addAndGet(size);
		}
		
		public void incView(int size) {
			this.viewed.addAndGet(size);
			
			this.incView.addAndGet(size);
		}
		
		public void incLike(int size) {
			this.liked.addAndGet(size);
			
			this.incLike.addAndGet(size);
		}
		
		public Long getLastUpdateTs() {
			return lastUpdateTs;
		}
		
		public void resetStat(ZUserStatistic obj) {
			this.subed.set(obj.getuSubedCount());
			this.viewed.set(obj.getuViewedCount());
			this.liked.set(obj.getuLikedCount());
			this.posted.set(obj.getuItemCount().intValue());
			
			this.lastUpdateTs = System.currentTimeMillis();
		}
		
		public long resetIncLike(){
			return this.incLike.getAndSet(0);
		}
		
		public long resetIncView(){
			return this.incView.getAndSet(0);
		}
		
		public long resetIncSub() {
			return this.incSub.getAndSet(0);
		}
		
		public long getIncView() {
			return incView.get();
		}
		
		public long getIncLike() {
			return incLike.get();
		}
		
		public long getIncSub() {
			return incSub.get();
		}
		
		public UserStatObj(){
			this.subed = new AtomicLong();
			this.viewed = new AtomicLong();
			this.liked = new AtomicLong();
			this.posted = new AtomicInteger();
			
			this.ts = System.currentTimeMillis() + rd.nextInt(14400000);
			this.lastUpdateTs = System.currentTimeMillis();
		}

		public UserStatObj(ZUserStatistic obj){
			this.subed = new AtomicLong(obj.getuSubedCount());
			this.viewed = new AtomicLong(obj.getuViewedCount());
			this.liked = new AtomicLong(obj.getuLikedCount());
			this.posted = new AtomicInteger(obj.getuItemCount().intValue());
			
			this.ts = System.currentTimeMillis() + rd.nextInt(21600000);
			this.lastUpdateTs = System.currentTimeMillis();
		}
		
		public UserStatisticObj getObj() {
			return new UserStatisticObj(this.getSubed(), this.getViewed(), this.getLiked(), this.getPosted());
		}
	}
}
