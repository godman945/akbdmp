package com.pchome.akbdmp.mongo.db.service.classcount;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.pchome.akbdmp.data.mongo.pojo.ClassCountMongoBean;
import com.pchome.akbdmp.mongo.db.dao.classcount.IClassCountDAO;
import com.pchome.akbdmp.mongo.db.service.base.BaseService;

@Service
public class ClassCountService extends BaseService<ClassCountMongoBean> implements IClassCountService {

	private static final long serialVersionUID = 1L;

	final static Logger log = Logger.getLogger(ClassCountService.class.getName());

	@Autowired
	IClassCountDAO classCountDAO;

	public ClassCountMongoBean saveOrUpdate(ClassCountMongoBean classCountMongoBean) throws Exception {
		return classCountDAO.saveOrUpdate(classCountMongoBean);
	}
}