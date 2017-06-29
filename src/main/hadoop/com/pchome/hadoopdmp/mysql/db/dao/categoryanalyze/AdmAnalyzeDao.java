package com.pchome.hadoopdmp.mysql.db.dao.categoryanalyze;



import org.hibernate.SessionFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import com.pchome.hadoopdmp.data.mysql.pojo.AdmCategoryAnalyze;
import com.pchome.hadoopdmp.mysql.db.dao.base.BaseDAO;


@Repository
public class AdmAnalyzeDao extends BaseDAO<AdmCategoryAnalyze, Integer>implements IAdmAnalyzeDao{

	@Autowired
	SessionFactory sessionFactory;
	
}
