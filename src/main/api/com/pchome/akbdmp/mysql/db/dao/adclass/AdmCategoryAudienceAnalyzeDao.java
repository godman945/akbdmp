package com.pchome.akbdmp.mysql.db.dao.adclass;



import java.util.List;

import org.hibernate.SessionFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import com.pchome.akbdmp.data.mysql.pojo.AdmCategoryAudienceAnalyze;
import com.pchome.akbdmp.mysql.db.dao.base.BaseDAO;


@Repository
public class AdmCategoryAudienceAnalyzeDao extends BaseDAO<AdmCategoryAudienceAnalyze, Integer>implements IAdmCategoryAudienceAnalyzeDao{

	@Autowired
	SessionFactory sessionFactory;


}
