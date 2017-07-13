package com.pchome.akbdmp.mysql.db.service.base;

import java.io.Serializable;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.hibernate.SharedSessionContract;
import org.hibernate.criterion.Projections;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.pchome.akbdmp.mysql.db.dao.base.IBaseDAO;
@Service
public abstract class BaseService<T, PK extends Serializable> implements IBaseService<T, PK> {
    protected Log log = LogFactory.getLog(this.getClass());

    @Autowired
    protected IBaseDAO<T, PK> dao;

    public T get(Serializable id) {
        return dao.get(id);
    }

    public long loadAllSize() {
        return dao.loadAllSize();
    }
    
    public List<T> findHql(String hql,Object[] value) {
        return dao.findHql(hql, value);
    }

    public List<T> loadAll() {
        return dao.loadAll();
    }

    public List<T> loadAll(int firstResult, int maxResults) {
        return dao.loadAll(firstResult, maxResults);
    }

    public PK save(T entity) {
        return dao.save(entity);
    }

    public void update(T entity) {
        dao.update(entity);
    }

    public void saveOrUpdate(T entity) {
        dao.saveOrUpdate(entity);
    }
    
    
    public void delete(T entity) {
        dao.delete(entity);
    }

    public IBaseDAO<T, PK> getDao() {
        return dao;
    }
    public void setDao(IBaseDAO<T, PK> dao) {
        this.dao = dao;
    }
    
    public List<T> findByPage(String hql, int page, int pageSize){
        return dao.findByPage(hql, page, pageSize);
    }
    
    public int rowCount(String hql) { 
    	return dao.rowCount(hql);
    }
    
    public List<Object> sqlFindByPage(String sql) {
    	return dao.sqlFindByPage(sql);
    }
    
    public int sqlRowCount(String sql) {
    	return dao.sqlRowCount(sql);
    }
}