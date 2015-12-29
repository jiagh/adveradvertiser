package jgh.util;


import jgh.vo.AdLog1_1;
import jgh.vo.Advertisers;
import jgh.vo.Cogtu_log_split;
import jgh.vo.ReqLog1_1;


@SuppressWarnings("serial")
public class Cogtu_log_splitTookit implements java.io.Serializable {


    
    public static Cogtu_log_split setCogtu_log_split(ReqLog1_1 rl, AdLog1_1 log) {
	Cogtu_log_split adv = new Cogtu_log_split();
  	adv.setAddrCode(rl.getAddrCode());
  	adv.setAdvertisersId(log.getAdvertisersId());
  	adv.setAgent(rl.getAgent());
  	adv.setBrowser(adv.getBrowser());
  	adv.setCampaignId(log.getCampaignId());
  	adv.setCity(rl.getCity());
  	adv.setCountry(rl.getCountry());
  	adv.setCreativeId(log.getCreativeId());
  	adv.setImpId(log.getImpId());
  	adv.setIp(rl.getIp());
  	adv.setOs(rl.getOs());
  	adv.setPageUri(rl.getPageUri());
  	adv.setPrice(log.getPrice());
  	adv.setPriceType(log.getPriceType());
  	adv.setProjectId(log.getProjectId());
  	adv.setProvince(rl.getProvince());
  	adv.setPublisherId(rl.getPublisherId());
  	adv.setReqType(rl.getReqType());
  	adv.setRunTime(rl.getRunTime());
  	adv.setSessionId(rl.getSessionId());
  	adv.setSiteId(rl.getSiteId());
  	adv.setSlotId(rl.getSlotId());
  	adv.setSource(rl.getSource());
  	adv.setTemplateId(log.getTemplateId());
  	adv.setTimestamp(rl.getTimestamp());
  	adv.setUid(rl.getUid());
  	adv.setV(rl.getV());
  	adv.setvBalanceCostPrecent(rl.getvBalanceCostPrecent());
  	adv.setRef(rl.getRef());
  	adv.setFeatures(String.valueOf(log.getFeatures()));
  	adv.setTags(String.valueOf(log.getTags()));
  	adv.setImpurl(log.getImpUrl());
  	
  	return adv;
      }
    public static String cogtu_log_splitTookitToString(Cogtu_log_split adv) {
	StringBuilder sb = new StringBuilder();
	String schar = "	";
	sb.append(adv.getIp()).append(schar)
	  .append(adv.getTimestamp()).append(schar)
	  .append(adv.getSlotId()).append(schar)
	  .append(adv.getSiteId()).append(schar)
	  .append(adv.getPublisherId()).append(schar)
	  .append(adv.getRef()).append(schar)
	  .append(adv.getCountry()).append(schar)
	  .append(adv.getProvince()).append(schar)
	  .append(adv.getCity()).append(schar)
	  .append(adv.getAddrCode()).append(schar)
	  .append(adv.getBrowser()).append(schar)
	  .append(adv.getOs()).append(schar)
	  .append(adv.getUid()).append(schar)
	  .append(adv.getRunTime()).append(schar)
	  .append(adv.getvBalanceCostPrecent()).append(schar)
	  .append(adv.getV()).append(schar)
	  .append(adv.getSessionId()).append(schar)
	  .append(adv.getSource()).append(schar)
	  .append(adv.getAgent()).append(schar)
	  .append(adv.getPageUri()).append(schar)
	  .append(adv.getReqType()).append(schar)
	  .append(adv.getImpId()).append(schar)
	  .append(adv.getImpurl()).append(schar)
	  .append(adv.getAdvertisersId()).append(schar)
	  .append(adv.getProjectId()).append(schar)
	  .append(adv.getCampaignId()).append(schar)
	  .append(adv.getCreativeId()).append(schar)
	  .append(adv.getTemplateId()).append(schar)
	  .append(adv.getPrice()).append(schar)
	  .append(adv.getPriceType()).append(schar)
	  .append(adv.getFeatures()).append(schar)
	  .append(adv.getTags()).append(schar);
	return sb.toString();
    }
    

}
