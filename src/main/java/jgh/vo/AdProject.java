package jgh.vo;

public class AdProject {

    private Integer campaignId = 0;
    private Integer creativeId = 0;
    private String slotId;
    private int camPv;
    private int camClick;
    private int creatPv;
    private int creatClick;
    private int soltPv;
    public Integer getCampaignId() {
        return campaignId;
    }
    public void setCampaignId(Integer campaignId) {
        this.campaignId = campaignId;
    }
    public Integer getCreativeId() {
        return creativeId;
    }
    public void setCreativeId(Integer creativeId) {
        this.creativeId = creativeId;
    }
    public String getSlotId() {
        return slotId;
    }
    public void setSlotId(String slotId) {
        this.slotId = slotId;
    }
    public int getCamPv() {
        return camPv;
    }
    public void setCamPv(int camPv) {
        this.camPv = camPv;
    }
    public int getCamClick() {
        return camClick;
    }
    public void setCamClick(int camClick) {
        this.camClick = camClick;
    }
    public int getCreatPv() {
        return creatPv;
    }
    public void setCreatPv(int creatPv) {
        this.creatPv = creatPv;
    }
    public int getCreatClick() {
        return creatClick;
    }
    public void setCreatClick(int creatClick) {
        this.creatClick = creatClick;
    }
    public int getSoltPv() {
        return soltPv;
    }
    public void setSoltPv(int soltPv) {
        this.soltPv = soltPv;
    }
    public int getSoltClick() {
        return soltClick;
    }
    public void setSoltClick(int soltClick) {
        this.soltClick = soltClick;
    }
    private int soltClick;
}
