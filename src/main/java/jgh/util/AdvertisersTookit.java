package jgh.util;


import jgh.vo.AdLog1_1;
import jgh.vo.Advertisers;
import jgh.vo.ReqLog1_1;


@SuppressWarnings("serial")
public class AdvertisersTookit implements java.io.Serializable {


    
    public static Advertisers setAdvertisers(ReqLog1_1 rl, AdLog1_1 log) {
  	Advertisers adv = new Advertisers();
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
  	return adv;
      }
    public static String advertisersToString(Advertisers adv) {
	StringBuilder sb = new StringBuilder();
	String schar = "$";
	sb.append(adv.getAddrCode()).append(schar).append(adv.getAdvertisersId()).append(schar).append(adv.getAgent()).append(schar).append(adv.getBrowser()).append(schar)
		.append(adv.getCampaignId()).append(schar).append(adv.getCity()).append(schar).append(adv.getCountry()).append(schar).append(adv.getCreativeId()).append(schar)
		.append(adv.getImpId()).append(schar).append(adv.getIp()).append(schar).append(adv.getOs()).append(schar).append(adv.getPageUri()).append(schar)
		.append(adv.getPrice()).append(schar).append(adv.getPriceType()).append(schar).append(adv.getProjectId()).append(schar).append(adv.getProvince()).append(schar)
		.append(adv.getPublisherId()).append(schar).append(adv.getReqType()).append(schar).append(adv.getRunTime()).append(schar).append(adv.getSessionId()).append(schar)
		.append(adv.getSiteId()).append(schar).append(adv.getSlotId()).append(schar).append(adv.getSource()).append(schar).append(adv.getTemplateId()).append(schar)
		.append(adv.getTimestamp()).append(schar).append(adv.getUid()).append(schar).append(adv.getV()).append(schar).append(adv.getvBalanceCostPrecent()).append(schar);
	return sb.toString();
    }
    public static Advertisers getAdvertisers(String[] str){
	Advertisers adv=new Advertisers();
	adv.setAddrCode(str[0]);
  	adv.setAdvertisersId(Integer.parseInt(str[1]));
  	adv.setAgent(str[2]);
  	adv.setBrowser(str[3]);
  	adv.setCampaignId(Integer.parseInt(str[4]));
  	adv.setCity(str[5]);
  	adv.setCountry(str[6]);
  	adv.setCreativeId(Integer.parseInt(str[7]));
  	adv.setImpId(str[8]);
  	adv.setIp(str[9]);
  	adv.setOs(str[10]);
  	adv.setPageUri(str[11]);
  	adv.setPrice(Long.parseLong(str[12]));
  	adv.setPriceType(str[13]);
  	adv.setProjectId(Integer.parseInt(str[14]));
  	adv.setProvince(str[15]);
  	adv.setPublisherId(Integer.parseInt(str[16]));
  	adv.setReqType(Integer.parseInt(str[17]));
  	adv.setRunTime(Long.parseLong(str[18]));
  	adv.setSessionId(str[19]);
  	adv.setSiteId(Integer.parseInt(str[20]));
  	adv.setSlotId(str[21]);
  	adv.setSource(str[22]);
  	adv.setTemplateId(Integer.parseInt(str[23]));
  	adv.setTimestamp(Long.parseLong(str[24]));
  	adv.setUid(str[25]);
  	adv.setV(str[26]);
  	adv.setvBalanceCostPrecent(Integer.parseInt(str[27]));
	return adv;
    }

}
