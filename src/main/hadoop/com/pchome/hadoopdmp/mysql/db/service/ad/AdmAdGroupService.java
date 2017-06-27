package com.pchome.hadoopdmp.mysql.db.service.ad;


import org.springframework.stereotype.Service;

import com.pchome.hadoopdmp.data.mysql.pojo.AdmAdGroup;
import com.pchome.hadoopdmp.mysql.db.service.base.BaseService;

@Service
public class AdmAdGroupService extends BaseService<AdmAdGroup, Integer> implements IAdmAdGroupService{

//	private List<PcsUserRecommendCategoryBean> womensRecommend = new ArrayList<PcsUserRecommendCategoryBean>();
//	private List<PcsUserRecommendCategoryBean> mensRecommend = new ArrayList<PcsUserRecommendCategoryBean>();
//	private List<PcsUserRecommendCategoryBean> kidsRecommend = new ArrayList<PcsUserRecommendCategoryBean>();
//	private List<PcsUserRecommendCategoryBean> beautyRecommend = new ArrayList<PcsUserRecommendCategoryBean>();
//	private List<PcsUserRecommendCategoryBean> petRecommend = new ArrayList<PcsUserRecommendCategoryBean>();
//	private List<PcsUserRecommendCategoryBean> homeRecommend = new ArrayList<PcsUserRecommendCategoryBean>();
//	private List<PcsUserRecommendCategoryBean> kitchenRecommend = new ArrayList<PcsUserRecommendCategoryBean>();
//	private List<PcsUserRecommendCategoryBean> electronicsRecommend = new ArrayList<PcsUserRecommendCategoryBean>();
//	private List<PcsUserRecommendCategoryBean> computersRecommend = new ArrayList<PcsUserRecommendCategoryBean>();
//	private List<PcsUserRecommendCategoryBean> phonesRecommend = new ArrayList<PcsUserRecommendCategoryBean>();
//	private List<PcsUserRecommendCategoryBean> healthRecommend = new ArrayList<PcsUserRecommendCategoryBean>();
//	private List<PcsUserRecommendCategoryBean> designRecommend = new ArrayList<PcsUserRecommendCategoryBean>();
//	private List<PcsUserRecommendCategoryBean> groceriesRecommend = new ArrayList<PcsUserRecommendCategoryBean>();
//	private List<PcsUserRecommendCategoryBean> booksRecommend = new ArrayList<PcsUserRecommendCategoryBean>();
//	private List<PcsUserRecommendCategoryBean> toysRecommend = new ArrayList<PcsUserRecommendCategoryBean>();
//	private List<PcsUserRecommendCategoryBean> videoGamesRecommend = new ArrayList<PcsUserRecommendCategoryBean>();
//	private List<PcsUserRecommendCategoryBean> sportsRecommend = new ArrayList<PcsUserRecommendCategoryBean>();
//	private List<PcsUserRecommendCategoryBean> cameraRecommend = new ArrayList<PcsUserRecommendCategoryBean>();
//	private List<PcsUserRecommendCategoryBean> celebrityRecommend = new ArrayList<PcsUserRecommendCategoryBean>();
//	private List<PcsUserRecommendCategoryBean> musicRecommend = new ArrayList<PcsUserRecommendCategoryBean>();
//	private List<PcsUserRecommendCategoryBean> procurementRecommend = new ArrayList<PcsUserRecommendCategoryBean>();
//	private List<PcsUserRecommendCategoryBean> rentalRecommend = new ArrayList<PcsUserRecommendCategoryBean>();
//	private List<PcsUserRecommendCategoryBean> couponsRecommend = new ArrayList<PcsUserRecommendCategoryBean>();
//	private List<PcsUserRecommendCategoryBean> snacksRecommend = new ArrayList<PcsUserRecommendCategoryBean>();
//	
//	
//	@Autowired
//	IPcsUserDao pcsUserDao;
//	
//	@Autowired
//	IPcsUserLoginInfoService pcsUserLoginInfoService;
//
//	@Autowired
//	IPcsUserFbInfoService pcsUserFbInfoService;
//	
//	@Autowired
//	IPcsUserRelationService pcsUserRelationService;
//	
//	@Autowired
//	IPcsUserProdService pcsUserProdService;
//	
//	@Autowired
//	MongoDBUtil mongoDBUtil;
//	
//	
//	
//	/**
//	 * 關建字搜尋使用者
//	 * */
//	public PcsSearchUserQueryReturnData getPcsUserByKeyword(String pcsUserId,String keyword,int nextPage,int pageSize,String fromPcsUserId) throws Exception{
//		List<Object> listObj = pcsUserDao.getPcsUserByKeyword(pcsUserId,keyword);
//
//		
//		int start = (nextPage-1)*pageSize;
//		int end = nextPage*pageSize-1;
//		int totalSize = listObj.size();
//		
//		List<PcsSearchUserQueryBean> pcsSearchUserQueryList = new ArrayList<PcsSearchUserQueryBean>();
//		for (int i = start;i< listObj.size(); i++) {
//			if(i > end){
//				break;
//			}
//			Object[] obj = (Object[]) listObj.get(i);
//			PcsSearchUserQueryBean pcsSearchUserQueryBean = new PcsSearchUserQueryBean();
//			pcsSearchUserQueryBean.setPcsUserId(String.valueOf(obj[0]));
//			if(String.valueOf(obj[1]).equals(PcsUserLoginStatusEnum.USER_LOGIN_PCS.getType())){
//				pcsSearchUserQueryBean.setPcsLoginAccount(String.valueOf(obj[2]));
//			}
//			if(String.valueOf(obj[1]).equals(PcsUserLoginStatusEnum.USER_LOGIN_FB.getType())){
//				pcsSearchUserQueryBean.setPcsLoginAccount(String.valueOf(obj[2]).substring(0,String.valueOf(obj[2]).indexOf("@")));
//			}
//			pcsSearchUserQueryBean.setPcsPhotoPath(String.valueOf(obj[4]));
//			if(StringUtils.isNotBlank(fromPcsUserId)){
//				pcsSearchUserQueryBean.setRelationStatus(pcsUserRelationService.getPcsUserRelationByFromPcsUserId(String.valueOf(obj[0]), fromPcsUserId));
//			}
//			pcsSearchUserQueryBean.setPcsFirstName(StringUtils.isBlank(String.valueOf(obj[6])) ? "" : String.valueOf(obj[6]));
//			pcsSearchUserQueryBean.setPcsLastName(StringUtils.isBlank(String.valueOf(obj[7])) ? "" : String.valueOf(obj[7]));
//			pcsSearchUserQueryList.add(pcsSearchUserQueryBean);
//		}
//		
//		
//		
//		
//		
//		//推薦買家目前隨機之後由後台上線
//		List<PcsUser> pcsUserList = pcsUserDao.loadAll();
//		List<RecommendUserInfo> recommendUserInfoDataList = new ArrayList<RecommendUserInfo>();
//		String pcsLoginAccount="";
//		for (int i = 0; i < 3; i++) {
//			boolean test = true;
//			while(test){
//				PcsUser pcsUser = pcsUserList.get((int)(Math.random() * pcsUserList.size()));
//				List<PcsProdMongoBean> pcsProdMongoBeanList = pcsUserProdService.findProdByUserId(pcsUser.getPcsUserId());
//				if(pcsProdMongoBeanList.size() == 0){
//					continue;
//				}
//				PcsProdMongoBean pcsProdMongoBean = pcsProdMongoBeanList.get((int)(Math.random() * pcsProdMongoBeanList.size()));
//				RecommendUserInfo recommendUserInfo = new RecommendUserInfo();
//				recommendUserInfo.setCategory(pcsProdMongoBean.getProd_category_name());
//				if(pcsUser.getLoginType().equals(PcsUserLoginStatusEnum.USER_LOGIN_PCS.getType())){
//					pcsLoginAccount = ((PcsUserLoginInfo)pcsUserLoginInfoService.getPcsUserLoginInfoByPcsUserId(pcsUser.getPcsUserId())).getPcsLoginAccount();
//				}
//				if(pcsUser.getLoginType().equals(PcsUserLoginStatusEnum.USER_LOGIN_FB.getType())){
//					pcsLoginAccount = ((PcsUserFbInfo)pcsUserFbInfoService.get(pcsUser.getPcsBindFbid())).getFbEmail();
//					pcsLoginAccount = pcsLoginAccount.substring(0,pcsLoginAccount.indexOf("@"));
//				}
//				recommendUserInfo.setPcsFirstName(StringUtils.isBlank(pcsUser.getPcsFirstName()) ? "" : pcsUser.getPcsFirstName());
//				recommendUserInfo.setPcsLastName(StringUtils.isBlank(pcsUser.getPcsLastName()) ? "" : pcsUser.getPcsLastName());
//				recommendUserInfo.setPcsLoginAccount(pcsLoginAccount);
//				recommendUserInfo.setPcsUserId(pcsUser.getPcsUserId());
//				recommendUserInfo.setPcsPhotoPath(StringUtils.isBlank(pcsUser.getPcsPhotoPath()) ? "" : pcsUser.getPcsPhotoPath());
//				if(StringUtils.isNotBlank(fromPcsUserId)){
//					recommendUserInfo.setRelationStatus(pcsUserRelationService.getPcsUserRelationByFromPcsUserId(pcsUser.getPcsUserId(), fromPcsUserId));
//				}
//				recommendUserInfo.setFansNum(0);
//				recommendUserInfo.setProdNum(0);
//				recommendUserInfoDataList.add(recommendUserInfo);
//				test = false;
//			}
//		}  
//		
//		String isNext ="";
//		if(end == totalSize-1 || end >= totalSize){
//			isNext = "true";
//		}else{
//			isNext = "false";
//		}
//		
//		PcsSearchUserQueryReturnData pcsSearchUserQueryReturnData = new PcsSearchUserQueryReturnData();
//		pcsSearchUserQueryReturnData.setIsNext(isNext);
//		pcsSearchUserQueryReturnData.setUserInfoData(pcsSearchUserQueryList);
//		pcsSearchUserQueryReturnData.setRecommendUserInfoData(recommendUserInfoDataList);
//		pcsSearchUserQueryReturnData.setTotalProdSize((int)totalSize);
//
//		return pcsSearchUserQueryReturnData;
//	}
//	
//	public boolean checkPcsUserId(String pcsUserId) throws Exception{
//		return pcsUserDao.checkPcsUserId(pcsUserId);
//	}
//	
//	public PcsUser getPcsUserByFbid(String fbid) throws Exception{
//		return pcsUserDao.getPcsUserByFbid(fbid);
//	}
//	
//	public List<PcsUser> getPcsUserByFbidList(String fbid) throws Exception{
//		return pcsUserDao.getPcsUserByFbidList(fbid);
//	}
//	
//	/**
//	 * 根據FBid查詢使用者列表
//	 * */
//	public PcsUserFbidReturnData getPcsUserInByFbidArray(JSONArray fbIdJsonArray,String fromPcsUserId,int nextPage,int pageSize) throws Exception{
//		
//		List<PcsSearchUserQueryBean> pcsSearchUserQueryList = new ArrayList<PcsSearchUserQueryBean>();
//		String pcsLoginAccount="";
//		for (Object obj : fbIdJsonArray) {
//			PcsSearchUserQueryBean pcsSearchUserQueryBean = new PcsSearchUserQueryBean();
//			PcsUser pcsUser = getPcsUserByFbid(obj.toString());
//			if(pcsUser == null){
//				continue;
//			}
//			if(pcsUser.getLoginType().equals(PcsUserLoginStatusEnum.USER_LOGIN_PCS.getType())){
//				pcsLoginAccount = ((PcsUserLoginInfo)pcsUserLoginInfoService.getPcsUserLoginInfoByPcsUserId(pcsUser.getPcsUserId())).getPcsLoginAccount();
//			}
//			if(pcsUser.getLoginType().equals(PcsUserLoginStatusEnum.USER_LOGIN_FB.getType())){
//				pcsLoginAccount = ((PcsUserFbInfo)pcsUserFbInfoService.get(pcsUser.getPcsBindFbid())).getFbEmail();
//				pcsLoginAccount = pcsLoginAccount.substring(0,pcsLoginAccount.indexOf("@"));
//			}
//			pcsSearchUserQueryBean.setPcsFirstName(StringUtils.isBlank(pcsUser.getPcsFirstName()) ? "" : pcsUser.getPcsFirstName());
//			pcsSearchUserQueryBean.setPcsLastName(StringUtils.isBlank(pcsUser.getPcsLastName()) ? "" : pcsUser.getPcsLastName());
//			pcsSearchUserQueryBean.setPcsLoginAccount(pcsLoginAccount);
//			pcsSearchUserQueryBean.setPcsUserId(pcsUser.getPcsUserId());
//			pcsSearchUserQueryBean.setPcsPhotoPath(StringUtils.isBlank(pcsUser.getPcsPhotoPath()) ? "" : pcsUser.getPcsPhotoPath());
//			if(StringUtils.isNotBlank(fromPcsUserId)){
//				pcsSearchUserQueryBean.setRelationStatus(pcsUserRelationService.getPcsUserRelationByFromPcsUserId(pcsUser.getPcsUserId(), fromPcsUserId));
//			}
//			pcsSearchUserQueryList.add(pcsSearchUserQueryBean);
//		}
//		
//		int start = (nextPage-1)*pageSize;
//		int end = nextPage*pageSize-1;
//		int totalSize = pcsSearchUserQueryList.size();
//		
//		List<PcsSearchUserQueryBean> pcsSearchUserQueryInfoList = new ArrayList<PcsSearchUserQueryBean>();
//		for (int i = start; i < pcsSearchUserQueryList.size(); i++) {
//			if(i > end){
//				break;
//			}
//			pcsSearchUserQueryInfoList.add(pcsSearchUserQueryList.get(i));
//		}
//		PcsUserFbidReturnData pcsUserFbidReturnData = new PcsUserFbidReturnData();
//		pcsUserFbidReturnData.setUserInfoData(pcsSearchUserQueryInfoList);
//		pcsUserFbidReturnData.setTotalProdSize(totalSize);
//		if (end == pcsSearchUserQueryList.size()-1) {
//			pcsUserFbidReturnData.setIsNext("false");
//		} else {
//			pcsUserFbidReturnData.setIsNext("true");
//		}
//		return pcsUserFbidReturnData;
//	}
//	
//	
//	/**
//	 * 1.自後台取得推薦賣家資料
//	 * 2.目前無後台暫時隨機
//	 * */
//	public PcsUserRecommendCategoryRetuenData getRecommendCategory(String fromPcsUserId) throws Exception{
//		this.womensRecommend.clear();
//		this.mensRecommend.clear();
//		this.kidsRecommend.clear();
//		this.beautyRecommend.clear();
//		this.petRecommend.clear();
//		this.homeRecommend.clear();
//		this.kitchenRecommend.clear();
//		this.electronicsRecommend.clear();
//		this.computersRecommend.clear();
//		this.phonesRecommend.clear();
//		this.healthRecommend.clear();
//		this.designRecommend.clear();
//		this.groceriesRecommend.clear();
//		this.booksRecommend.clear();
//		this.toysRecommend.clear();
//		this.videoGamesRecommend.clear();
//		this.sportsRecommend.clear();
//		this.cameraRecommend.clear();
//		this.celebrityRecommend.clear();
//		this.musicRecommend.clear();
//		this.procurementRecommend.clear();
//		this.rentalRecommend.clear();
//		this.couponsRecommend.clear();
//		this.snacksRecommend.clear();
//		
//		List<PcsUserProd> pcsUserProdList = pcsUserProdService.loadAll();
//		for (PcsUserProd pcsUserProd : pcsUserProdList) {
//			
//			if(womensRecommend.size() > 1 && pcsUserProd.getPcsProdCategory().getProdCategoryId() == 1){
//				continue;
//			}
//			if(mensRecommend.size() > 1 && pcsUserProd.getPcsProdCategory().getProdCategoryId() == 2){
//				continue;
//			}
//			if(kidsRecommend.size() > 1 && pcsUserProd.getPcsProdCategory().getProdCategoryId() == 3){
//				continue;
//			}
//			if(beautyRecommend.size() > 1 && pcsUserProd.getPcsProdCategory().getProdCategoryId() == 4){
//				continue;
//			}
//			if(petRecommend.size() > 1 && pcsUserProd.getPcsProdCategory().getProdCategoryId() == 5){
//				continue;
//			}
//			if(homeRecommend.size() > 1 && pcsUserProd.getPcsProdCategory().getProdCategoryId() == 6){
//				continue;
//			}
//			if(kitchenRecommend.size() > 1 && pcsUserProd.getPcsProdCategory().getProdCategoryId() == 7){
//				continue;
//			}
//			if(electronicsRecommend.size() > 1 && pcsUserProd.getPcsProdCategory().getProdCategoryId() == 8){
//				continue;
//			}
//			if(computersRecommend.size() > 1 && pcsUserProd.getPcsProdCategory().getProdCategoryId() == 9){
//				continue;
//			}
//			if(phonesRecommend.size() > 1 && pcsUserProd.getPcsProdCategory().getProdCategoryId() == 10){
//				continue;
//			}
//			if(healthRecommend.size() > 1 && pcsUserProd.getPcsProdCategory().getProdCategoryId() == 11){
//				continue;
//			}
//			if(designRecommend.size() > 1 && pcsUserProd.getPcsProdCategory().getProdCategoryId() == 12){
//				continue;
//			}
//			if(groceriesRecommend.size() > 1 && pcsUserProd.getPcsProdCategory().getProdCategoryId() == 13){
//				continue;
//			}
//			if(booksRecommend.size() > 1 && pcsUserProd.getPcsProdCategory().getProdCategoryId() == 14){
//				continue;
//			}
//			if(toysRecommend.size() > 1 && pcsUserProd.getPcsProdCategory().getProdCategoryId() == 15){
//				continue;
//			}
//			if(videoGamesRecommend.size() > 1 && pcsUserProd.getPcsProdCategory().getProdCategoryId() == 16){
//				continue;
//			}
//			if(sportsRecommend.size() > 1 && pcsUserProd.getPcsProdCategory().getProdCategoryId() == 17){
//				continue;
//			}
//			if(cameraRecommend.size() > 1 && pcsUserProd.getPcsProdCategory().getProdCategoryId() == 18){
//				continue;
//			}
//			if(celebrityRecommend.size() > 1 && pcsUserProd.getPcsProdCategory().getProdCategoryId() == 19){
//				continue;
//			}
//			if(musicRecommend.size() > 1 && pcsUserProd.getPcsProdCategory().getProdCategoryId() == 20){
//				continue;
//			}
//			if(procurementRecommend.size() > 1 && pcsUserProd.getPcsProdCategory().getProdCategoryId() == 21){
//				continue;
//			}
//			if(rentalRecommend.size() > 1 && pcsUserProd.getPcsProdCategory().getProdCategoryId() == 22){
//				continue;
//			}
//			if(couponsRecommend.size() > 1 && pcsUserProd.getPcsProdCategory().getProdCategoryId() == 23)  {
//				continue;
//			}
//			if(snacksRecommend.size() > 1 && pcsUserProd.getPcsProdCategory().getProdCategoryId() == 24){
//				continue;
//			}
//			setRecommendCategoryInfo(pcsUserProd,fromPcsUserId);
//		}
//		PcsUserRecommendCategoryRetuenData pcsUserRecommendCategoryRetuenData = new PcsUserRecommendCategoryRetuenData();
//		pcsUserRecommendCategoryRetuenData.setBeautyRecommend(beautyRecommend);
//		pcsUserRecommendCategoryRetuenData.setBooksRecommend(booksRecommend);
//		pcsUserRecommendCategoryRetuenData.setCameraRecommend(cameraRecommend);
//		pcsUserRecommendCategoryRetuenData.setCelebrityRecommend(celebrityRecommend);
//		pcsUserRecommendCategoryRetuenData.setComputersRecommend(computersRecommend);
//		pcsUserRecommendCategoryRetuenData.setCouponsRecommend(couponsRecommend);
//		pcsUserRecommendCategoryRetuenData.setDesignRecommend(designRecommend);
//		pcsUserRecommendCategoryRetuenData.setElectronicsRecommend(electronicsRecommend);
//		pcsUserRecommendCategoryRetuenData.setGroceriesRecommend(groceriesRecommend);
//		pcsUserRecommendCategoryRetuenData.setHealthRecommend(healthRecommend);
//		pcsUserRecommendCategoryRetuenData.setHomeRecommend(homeRecommend);
//		pcsUserRecommendCategoryRetuenData.setKidsRecommend(kidsRecommend);
//		pcsUserRecommendCategoryRetuenData.setKitchenRecommend(kitchenRecommend);
//		pcsUserRecommendCategoryRetuenData.setMensRecommend(mensRecommend);
//		pcsUserRecommendCategoryRetuenData.setMusicRecommend(musicRecommend);
//		pcsUserRecommendCategoryRetuenData.setPetRecommend(petRecommend);
//		pcsUserRecommendCategoryRetuenData.setPhonesRecommend(phonesRecommend);
//		pcsUserRecommendCategoryRetuenData.setProcurementRecommend(procurementRecommend);
//		pcsUserRecommendCategoryRetuenData.setRentalRecommend(rentalRecommend);
//		pcsUserRecommendCategoryRetuenData.setSnacksRecommend(snacksRecommend);
//		pcsUserRecommendCategoryRetuenData.setSportsRecommend(sportsRecommend);
//		pcsUserRecommendCategoryRetuenData.setToysRecommend(toysRecommend);
//		pcsUserRecommendCategoryRetuenData.setVideoGamesRecommend(videoGamesRecommend);
//		pcsUserRecommendCategoryRetuenData.setWomensRecommend(womensRecommend);
//		return pcsUserRecommendCategoryRetuenData;
//	}
//	
//	
//	//模擬用
//	public void setRecommendCategoryInfo(PcsUserProd pcsUserProd,String fromPcsUserId) throws Exception{
//		
//		PcsUserRecommendCategoryBean pcsUserRecommendCategoryBean = new PcsUserRecommendCategoryBean();
//		pcsUserRecommendCategoryBean.setFansNum("0");
//		pcsUserRecommendCategoryBean.setProdNum("0");
//		pcsUserRecommendCategoryBean.setPcsFirstName(StringUtils.isBlank(pcsUserProd.getPcsUser().getPcsFirstName()) ? "" : pcsUserProd.getPcsUser().getPcsFirstName());
//		pcsUserRecommendCategoryBean.setPcsLastName(StringUtils.isBlank(pcsUserProd.getPcsUser().getPcsLastName()) ? "" : pcsUserProd.getPcsUser().getPcsLastName());
//		pcsUserRecommendCategoryBean.setPcsPhotoPath(pcsUserProd.getPcsUser().getPcsPhotoPath());
//		pcsUserRecommendCategoryBean.setProdCategoryId(pcsUserProd.getPcsProdCategory().getProdCategoryId());
//		pcsUserRecommendCategoryBean.setPcsProdCategory(pcsUserProd.getPcsProdCategory().getPcsProdCategory());
//		pcsUserRecommendCategoryBean.setPcsUserId(pcsUserProd.getPcsUser().getPcsUserId());
//		if(StringUtils.isNotBlank(fromPcsUserId)){
//			pcsUserRecommendCategoryBean.setRelationStatus(pcsUserRelationService.getPcsUserRelationByFromPcsUserId(pcsUserProd.getPcsUser().getPcsUserId(), fromPcsUserId));	
//		}else{
//			pcsUserRecommendCategoryBean.setRelationStatus(0);
//		}
//		String pcsLoginAccount="";
//		if(pcsUserProd.getPcsUser().getLoginType().equals(PcsUserLoginStatusEnum.USER_LOGIN_PCS.getType())){
//			pcsLoginAccount = ((PcsUserLoginInfo)pcsUserLoginInfoService.getPcsUserLoginInfoByPcsUserId(pcsUserProd.getPcsUser().getPcsUserId())).getPcsLoginAccount();
//		}
//		if(pcsUserProd.getPcsUser().getLoginType().equals(PcsUserLoginStatusEnum.USER_LOGIN_FB.getType())){
//			pcsLoginAccount = ((PcsUserFbInfo)pcsUserFbInfoService.get(pcsUserProd.getPcsUser().getPcsBindFbid())).getFbEmail();
//			pcsLoginAccount = pcsLoginAccount.substring(0,pcsLoginAccount.indexOf("@"));
//		}
//		pcsUserRecommendCategoryBean.setPcsLoginAccount(pcsLoginAccount);
//		
//		if(pcsUserProd.getPcsProdCategory().getProdCategoryId() == 1){
//			womensRecommend.add(pcsUserRecommendCategoryBean);
//		}
//		if(pcsUserProd.getPcsProdCategory().getProdCategoryId() == 2){
//			mensRecommend.add(pcsUserRecommendCategoryBean);
//		}
//		if(pcsUserProd.getPcsProdCategory().getProdCategoryId() == 3){
//			kidsRecommend.add(pcsUserRecommendCategoryBean);
//		}
//		if(pcsUserProd.getPcsProdCategory().getProdCategoryId() == 4){
//			beautyRecommend.add(pcsUserRecommendCategoryBean);
//		}
//		if(pcsUserProd.getPcsProdCategory().getProdCategoryId() == 5){
//			petRecommend.add(pcsUserRecommendCategoryBean);
//		}
//		if(pcsUserProd.getPcsProdCategory().getProdCategoryId() == 6){
//			homeRecommend.add(pcsUserRecommendCategoryBean);
//		}
//		if(pcsUserProd.getPcsProdCategory().getProdCategoryId() == 7){
//			kitchenRecommend.add(pcsUserRecommendCategoryBean);
//		}
//		if(pcsUserProd.getPcsProdCategory().getProdCategoryId() == 8){
//			electronicsRecommend.add(pcsUserRecommendCategoryBean);
//		}
//		if(pcsUserProd.getPcsProdCategory().getProdCategoryId() == 9){
//			computersRecommend.add(pcsUserRecommendCategoryBean);
//		}
//		if(pcsUserProd.getPcsProdCategory().getProdCategoryId() == 10){
//			phonesRecommend.add(pcsUserRecommendCategoryBean);
//		}
//		if(pcsUserProd.getPcsProdCategory().getProdCategoryId() == 11){
//			healthRecommend.add(pcsUserRecommendCategoryBean);
//		}
//		if(pcsUserProd.getPcsProdCategory().getProdCategoryId() == 12){
//			designRecommend.add(pcsUserRecommendCategoryBean);
//		}
//		if(pcsUserProd.getPcsProdCategory().getProdCategoryId() == 13){
//			groceriesRecommend.add(pcsUserRecommendCategoryBean);
//		}
//		if(pcsUserProd.getPcsProdCategory().getProdCategoryId() == 14){
//			booksRecommend.add(pcsUserRecommendCategoryBean);
//		}
//		if(pcsUserProd.getPcsProdCategory().getProdCategoryId() == 15){
//			toysRecommend.add(pcsUserRecommendCategoryBean);
//		}
//		if(pcsUserProd.getPcsProdCategory().getProdCategoryId() == 16){
//			videoGamesRecommend.add(pcsUserRecommendCategoryBean);
//		}
//		if(pcsUserProd.getPcsProdCategory().getProdCategoryId() == 17){
//			sportsRecommend.add(pcsUserRecommendCategoryBean);
//		}
//		if(pcsUserProd.getPcsProdCategory().getProdCategoryId() == 18){
//			cameraRecommend.add(pcsUserRecommendCategoryBean);
//		}
//		if(pcsUserProd.getPcsProdCategory().getProdCategoryId() == 19){
//			celebrityRecommend.add(pcsUserRecommendCategoryBean);
//		}
//		if(pcsUserProd.getPcsProdCategory().getProdCategoryId() == 20){
//			musicRecommend.add(pcsUserRecommendCategoryBean);
//		}
//		if(pcsUserProd.getPcsProdCategory().getProdCategoryId() == 21){
//			procurementRecommend.add(pcsUserRecommendCategoryBean);
//		}
//		if(pcsUserProd.getPcsProdCategory().getProdCategoryId() == 22){
//			rentalRecommend.add(pcsUserRecommendCategoryBean);
//		}
//		if(pcsUserProd.getPcsProdCategory().getProdCategoryId() == 23){
//			couponsRecommend.add(pcsUserRecommendCategoryBean);
//		}
//		if(pcsUserProd.getPcsProdCategory().getProdCategoryId() == 24){
//			snacksRecommend.add(pcsUserRecommendCategoryBean);
//		}
//	}
	
}
