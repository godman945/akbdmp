package com.pchome.hadoopdmp.mysql.db.dao.category;



import org.hibernate.SessionFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import com.pchome.hadoopdmp.data.mysql.pojo.AdmCategoryGroup;
import com.pchome.hadoopdmp.mysql.db.dao.base.BaseDAO;


@Repository
public class AdmCategoryGroupDao extends BaseDAO<AdmCategoryGroup, Integer>implements IAdmCategoryGroupDao{

	@Autowired
	SessionFactory sessionFactory;
	
}
