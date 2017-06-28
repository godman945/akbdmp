package com.pchome.hadoopdmp.mysql.db.dao.categoryanalyze;



import org.hibernate.SessionFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import com.pchome.hadoopdmp.data.mysql.pojo.AdmCategoryGroupAnalyze;
import com.pchome.hadoopdmp.mysql.db.dao.base.BaseDAO;


@Repository
public class AdmGroupAnalyzeDao extends BaseDAO<AdmCategoryGroupAnalyze, Integer>implements IAdmGroupAnalyzeDao{

	@Autowired
	SessionFactory sessionFactory;
	
}
