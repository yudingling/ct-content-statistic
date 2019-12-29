package com.ct.content.statistic.component;

import io.netty.util.internal.ConcurrentSet;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.PostConstruct;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.ct.common.model.cache.TopicStatisticCacheType;
import com.ct.common.model.db.ZTopicStatistic;
import com.ct.common.model.mq.TopicStatisticData;
import com.ct.common.model.statistic.TopicStatisticObj;
import com.ct.content.statistic.service.TopicStatisticService;
import com.zeasn.common.cache.LocalCacheListener;
import com.zeasn.common.log.MyLog;
import com.zeasn.common.log.RuntimeLog;
import com.zeasn.common.model.cache.LocalCacheAction;
import com.zeasn.common.util.Common;

@Component
public class TopicStatistic extends LocalCacheListener<TopicStatisticCacheType> {
	private static final MyLog log = MyLog.getLog(TopicStatistic.class);
	
	private final ConcurrentHashMap<Long, TpStat> cache = new ConcurrentHashMap<>();
	private static final long LOCAL_EXPIRE_MS = 3600000; //expire in 60 mins
	private static final long UPDATE_DURATION_MS = 120000; //update in 2 min
	
	@Autowired
	private TopicStatisticService statisticService;
	
	private Random rd = new Random();
	
	@PostConstruct
	private void init(){
		new Thread(this::clearExpire).start();
		new Thread(this::updateInner).start();
	}
	
	public TopicStatistic() {
		super(TopicStatisticCacheType.class);
	}
	
	@Override
	public void onChanged(LocalCacheAction cacheAction, Object msg) {
		TopicStatisticData data = this.getValidData(msg);
		switch(cacheAction){
			case UPDATE:
				if(data != null){
					this.updateData(data);
				}
				break;
				
			case DELETE:
			case ADD:
			case RELOAD:
				log.warn(RuntimeLog.build(data, "TopicStatistic doesn't support delete/add/reload action."));
				break;
				
			default:
				break;
		}
	}
	
	private TopicStatisticData getValidData(Object msg){
		return msg instanceof TopicStatisticData ? (TopicStatisticData) msg : null;
	}
	
	private void updateData(TopicStatisticData data) {
		TpStat stat = this.cache.get(data.getTopicId());
		if(stat != null) {
			if(data.getIncPosted() != 0) {
				stat.incPost(data.getIncPosted());
			}
			
			if(data.getIncFollowed() != 0) {
				stat.incFollow(data.getIncFollowed());
			}
		}
	}
	
	private void clearExpire(){
		Common.sleepWithInterrupt(rd.nextInt((int) 360000));
		
		while(!Thread.currentThread().isInterrupted()){
			if(!this.cache.isEmpty()){
				long cmpTs = System.currentTimeMillis() - LOCAL_EXPIRE_MS;
				
				this.cache.entrySet().parallelStream().forEach(item -> {
					TpStat stat = item.getValue();
					
					boolean canRemove = Math.abs(stat.getIncLike()) == 0 && Math.abs(stat.getIncView()) == 0;
					
					if(canRemove && cmpTs > stat.getTs()){
						this.cache.remove(item.getKey());
					}
				});
			}
			
			Common.sleepWithInterrupt(LOCAL_EXPIRE_MS);
		}
	}
	
	private void updateInner(){
		Common.sleepWithInterrupt(rd.nextInt((int) 60000));
		
		while(!Thread.currentThread().isInterrupted()){
			if(!this.cache.isEmpty()){
				Set<Long> idsForReset = new ConcurrentSet<>();
				
				Random rd = new Random();
				
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
	 * return the topic id which need to reset from db
	 */
	private Long incStatistic(Long topicId, TpStat stat, int uptThreshold){
		if(Math.abs(stat.getIncView()) >= uptThreshold || Math.abs(stat.getIncLike()) >= uptThreshold 
				|| (System.currentTimeMillis() - stat.getLastUpdateTs() > LOCAL_EXPIRE_MS)){
			
			Long incView = stat.resetIncView();
			Long incLike = stat.resetIncLike();
			
			if(incView != 0 || incLike != 0){
				this.statisticService.incStatistic(topicId, incView, incLike);
			}
			
			return topicId;
			
		}else{
			return null;
		}
	}
	
	private void resetLikeAndView(Set<Long> idsForReset){
		Set<Long> idList = new HashSet<>();
		for(Long topicId : idsForReset){
			if(idList.size() > 50){
				this.resetLikeAndViewInBatch(idList);
			}
			
			idList.add(topicId);
		}
		
		if(CollectionUtils.isNotEmpty(idList)){
			this.resetLikeAndViewInBatch(idList);
		}
	}
	
	private void resetLikeAndViewInBatch(Set<Long> topicIds){
		try{
			Map<Long, ZTopicStatistic> stats = this.statisticService.getStatistic(topicIds);
			if(MapUtils.isNotEmpty(stats)){
				stats.forEach((itemId, itemStat) -> {
					TpStat stat = this.cache.get(itemId);
					if(stat != null){
						stat.resetStat(itemStat);
					}
				});
			}
			
		}catch(Exception ex){
			log.error("update content statistic from db error: " + ex.getMessage(), ex);
		}
		
		topicIds.clear();
	}
	
	public TopicStatisticObj getTopicStatistic(Long topicId){
		TpStat obj = this.cache.get(topicId);
		
		if(obj == null){
			obj = this.queryFromDB(topicId);			
		}
		
		obj.refresh();
		
		return obj.getObj();
	}
	
	public Map<Long, TopicStatisticObj> getTopicStatistic(Set<Long> topicIds){
		Set<Long> idsForDB = new HashSet<>();
		Map<Long, TopicStatisticObj> map = new HashMap<>();
		
		topicIds.forEach(id -> {
			TpStat obj = this.cache.get(id);
			if(obj != null){
				obj.refresh();
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
	
	private TpStat createTopicStatistic(long topicId, ZTopicStatistic stat){
		if(stat != null) {
			return new TpStat(stat);
			
		}else {
			return new TpStat(new ZTopicStatistic(topicId, 0l, 0l, 0l, 0l));
		}
	}
	
	private void queryFromDB(Set<Long> idsForDB, Map<Long, TopicStatisticObj> map){
		Map<Long, ZTopicStatistic> statMap = this.statisticService.getStatistic(idsForDB);
		
		idsForDB.forEach(topicId -> {
			TpStat ss = this.createTopicStatistic(topicId, statMap.get(topicId));
			
			map.put(topicId, ss.getObj());
			this.cache.put(topicId, ss);
		});
	}
	
	private TpStat queryFromDB(long idForDB){
		ZTopicStatistic stat = this.statisticService.getStatistic(idForDB);
		
		TpStat ss = this.createTopicStatistic(idForDB, stat);
		
		this.cache.put(idForDB, ss);
		
		return ss;
	}
	
	public void incView(long topicId, int size){
		TpStat obj = this.getStatistic(topicId);
		
		obj.refresh();
		obj.incView(size);
	}
	
	public void incLike(Long topicId, int size){
		TpStat obj = this.getStatistic(topicId);
		
		obj.refresh();
		obj.incLike(size);
	}
	
	private TpStat getStatistic(Long topicId){
		return this.cache.computeIfAbsent(topicId, id -> {
			ZTopicStatistic stat = this.statisticService.getStatistic(topicId);
			
			return this.createTopicStatistic(topicId, stat);
		});
	}
	
	class TpStat{
		private AtomicLong viewCount;
		private AtomicLong likeCount;
		private AtomicLong followCount;
		private AtomicLong postCount;
		
		private AtomicLong incView;
		private AtomicLong incLike;
		private Long ts;
		
		private Long lastUpdateTs;
		
		public Long getViewCount() {
			Long tmp = viewCount.get();
			return tmp >= 0 ? tmp : 0;
		}
		public Long getLikeCount() {
			Long tmp = likeCount.get();
			return tmp >= 0 ? tmp : 0;
		}
		public Long getFollowCount() {
			Long tmp = followCount.get();
			return tmp >= 0 ? tmp : 0;
		}
		public Long getPostCount() {
			Long tmp = postCount.get();
			return tmp >= 0 ? tmp : 0;
		}
		
		public Long getLastUpdateTs() {
			return lastUpdateTs;
		}
		
		public Long getTs() {
			return ts;
		}
		
		public TpStat(ZTopicStatistic stat) {
			super();
			this.viewCount = new AtomicLong(stat.getTsViewed());
			this.likeCount = new AtomicLong(stat.getTsLiked());
			this.followCount = new AtomicLong(stat.getTsFollowed());
			this.postCount = new AtomicLong(stat.getTsPosted());
			
			this.incView = new AtomicLong();
			this.incLike = new AtomicLong();
			
			this.refresh();
			this.lastUpdateTs = System.currentTimeMillis();
		}
		
		public TopicStatisticObj getObj(){
			return new TopicStatisticObj(this.getViewCount(), this.getLikeCount(), this.getPostCount(), this.getFollowCount());
		}
		
		public void incView(int size){
			this.viewCount.addAndGet(size);
			this.incView.addAndGet(size);
		}
		
		public void incLike(int size){
			this.likeCount.addAndGet(size);
			this.incLike.addAndGet(size);
		}
		
		public void incPost(int size) {
			this.postCount.addAndGet(size);
		}
		
		public void  incFollow(int size) {
			this.followCount.addAndGet(size);
		}
		
		public void refresh(){
			this.ts = System.currentTimeMillis() + rd.nextInt(3600000);
		}
		
		public void resetStat(ZTopicStatistic stat){
			this.likeCount.set(stat.getTsLiked());
			this.viewCount.set(stat.getTsViewed());
			this.followCount.set(stat.getTsFollowed());
			this.postCount.set(stat.getTsPosted());
			
			this.lastUpdateTs = System.currentTimeMillis();
		}
		
		public Long resetIncLike(){
			return this.incLike.getAndSet(0);
		}
		
		public Long resetIncView(){
			return this.incView.getAndSet(0);
		}
		
		public Long getIncView() {
			return incView.get();
		}
		
		public Long getIncLike() {
			return incLike.get();
		}
	}
}
