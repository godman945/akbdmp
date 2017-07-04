package com.pchome.hadoopdmp.mysql.db.dao.category;



import org.hibernate.SessionFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import com.pchome.hadoopdmp.data.mysql.pojo.AdmCategoryAudienceAnalyze;
import com.pchome.hadoopdmp.mysql.db.dao.base.BaseDAO;


@Repository
public class AdmCategoryAudienceAnalyzeDao extends BaseDAO<AdmCategoryAudienceAnalyze, Integer>implements IAdmCategoryAudienceAnalyzeDao{

	@Autowired
	SessionFactory sessionFactory;
	
}
