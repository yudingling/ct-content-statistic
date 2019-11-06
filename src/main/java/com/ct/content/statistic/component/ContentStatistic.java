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

import com.ct.common.model.db.ZLauncherItemStatistic;
import com.ct.common.model.statistic.ItemStatisticObj;
import com.ct.content.statistic.service.ContentStatisticService;
import com.zeasn.common.log.MyLog;
import com.zeasn.common.util.Common;

@Component
public class ContentStatistic {
	private static final MyLog log = MyLog.getLog(ContentStatistic.class);
	
	private final ConcurrentHashMap<Long, ItemStatistic> cache = new ConcurrentHashMap<>();
	private static final long LOCAL_EXPIRE_MS = 3600000; //expire in 60 mins
	private static final long UPDATE_DURATION_MS = 120000; //update in 2 min
	
	@Autowired
	private ContentStatisticService statisticService;
	
	private Random rd = new Random();
	
	@PostConstruct
	private void init(){
		new Thread(this::clearExpire).start();
		new Thread(this::updateInner).start();
	}
	
	private void clearExpire(){
		Common.sleepWithInterrupt(rd.nextInt((int) 360000));
		
		while(!Thread.currentThread().isInterrupted()){
			if(!this.cache.isEmpty()){
				long cmpTs = System.currentTimeMillis() - LOCAL_EXPIRE_MS;
				
				this.cache.entrySet().parallelStream().forEach(item -> {
					ItemStatistic stat = item.getValue();
					
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
	 * return the item id which need to reset from db
	 */
	private Long incStatistic(Long itemId, ItemStatistic stat, int uptThreshold){
		if(Math.abs(stat.getIncView()) >= uptThreshold || Math.abs(stat.getIncLike()) >= uptThreshold 
				|| (System.currentTimeMillis() - stat.getLastUpdateTs() > LOCAL_EXPIRE_MS)){
			
			Long incView = stat.resetIncView();
			Long incLike = stat.resetIncLike();
			
			if(incView != 0 || incLike != 0){
				this.statisticService.incStatistic(itemId, incView, incLike);
			}
			
			return itemId;
			
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
	
	private void resetLikeAndViewInBatch(Set<Long> itemIds){
		try{
			Map<Long, ZLauncherItemStatistic> stats = this.statisticService.getStatistic(itemIds);
			if(MapUtils.isNotEmpty(stats)){
				stats.forEach((itemId, itemStat) -> {
					ItemStatistic stat = this.cache.get(itemId);
					if(stat != null){
						stat.resetStat(itemStat);
					}
				});
			}
			
		}catch(Exception ex){
			log.error("update content statistic from db error: " + ex.getMessage(), ex);
		}
		
		itemIds.clear();
	}
	
	public ItemStatisticObj getItemStatistic(Long itemId){
		ItemStatistic obj = this.cache.get(itemId);
		
		if(obj == null){
			obj = this.queryFromDB(itemId);			
		}
		
		obj.refresh();
		
		return obj.getObj();
	}
	
	public Map<Long, ItemStatisticObj> getItemStatistic(Set<Long> itemIds){
		Set<Long> idsForDB = new HashSet<>();
		Map<Long, ItemStatisticObj> map = new HashMap<>();
		
		itemIds.forEach(id -> {
			ItemStatistic obj = this.cache.get(id);
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
	
	private ItemStatistic createItemStatistic(ZLauncherItemStatistic stat){
		Long view = 0l;
		Long like = 0l;
		if(stat != null){
			view = stat.getItemViewcount();
			like = stat.getItemLikecount();
		}
		
		return new ItemStatistic(view, like);
	}
	
	private void queryFromDB(Set<Long> idsForDB, Map<Long, ItemStatisticObj> map){
		Map<Long, ZLauncherItemStatistic> statMap = this.statisticService.getStatistic(idsForDB);
		
		idsForDB.forEach(itemId -> {
			ItemStatistic ss = this.createItemStatistic(statMap.get(itemId));
			
			map.put(itemId, ss.getObj());
			this.cache.put(itemId, ss);
		});
	}
	
	private ItemStatistic queryFromDB(Long idForDB){
		ZLauncherItemStatistic stat = this.statisticService.getStatistic(idForDB);
		
		ItemStatistic ss = this.createItemStatistic(stat);
		
		this.cache.put(idForDB, ss);
		
		return ss;
	}
	
	public void incView(Long itemId, int size){
		ItemStatistic obj = this.getStatistic(itemId);
		
		obj.refresh();
		obj.incView(size);
	}
	
	public void incLike(Long itemId, int size){
		ItemStatistic obj = this.getStatistic(itemId);
		
		obj.refresh();
		obj.incLike(size);
	}
	
	private ItemStatistic getStatistic(Long itemId){
		return this.cache.computeIfAbsent(itemId, id -> {
			ZLauncherItemStatistic stat = this.statisticService.getStatistic(itemId);
			
			return this.createItemStatistic(stat);
		});
	}
	
	class ItemStatistic{
		private AtomicLong viewCount;
		private AtomicLong likeCount;
		
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
		
		public Long getLastUpdateTs() {
			return lastUpdateTs;
		}
		
		public Long getTs() {
			return ts;
		}
		
		public ItemStatistic(Long viewCount, Long likeCount) {
			super();
			this.viewCount = new AtomicLong(viewCount);
			this.likeCount = new AtomicLong(likeCount);
			
			this.incView = new AtomicLong();
			this.incLike = new AtomicLong();
			
			this.refresh();
			this.lastUpdateTs = System.currentTimeMillis();
		}
		
		public ItemStatisticObj getObj(){
			return new ItemStatisticObj(this.getViewCount(), this.getLikeCount());
		}
		
		public void incView(int size){
			this.viewCount.addAndGet(size);
			this.incView.addAndGet(size);
		}
		
		public void incLike(int size){
			this.likeCount.addAndGet(size);
			this.incLike.addAndGet(size);
		}
		
		public void refresh(){
			this.ts = System.currentTimeMillis() + rd.nextInt(3600000);
		}
		
		public void resetStat(ZLauncherItemStatistic stat){
			this.likeCount.set(stat.getItemLikecount());
			this.viewCount.set(stat.getItemViewcount());
			
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
