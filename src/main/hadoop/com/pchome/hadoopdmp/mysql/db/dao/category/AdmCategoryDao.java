package com.pchome.hadoopdmp.mysql.db.dao.category;



import org.hibernate.SessionFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import com.pchome.hadoopdmp.data.mysql.pojo.AdmCategory;
import com.pchome.hadoopdmp.mysql.db.dao.base.BaseDAO;


@Repository
public class AdmCategoryDao extends BaseDAO<AdmCategory, Integer>implements IAdmCategoryDao{

	@Autowired
	SessionFactory sessionFactory;
	
}
