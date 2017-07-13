package com.pchome.akbdmp.mysql.db.service.base;

import java.io.Serializable;
import java.util.List;

import org.springframework.stereotype.Service;
@Service
public interface IBaseService<T, PK extends Serializable> {
    
	public T get(Serializable id);

    public long loadAllSize();

    public List<T> loadAll();
    
    public List<T> findHql(String hql,Object[] value);

    public List<T> loadAll(int firstResult, int maxResults);

    public PK save(T entity);

    public void update(T entity);

    public void saveOrUpdate(T entity);

    public void delete(T entity);

    public List<T> findByPage(String hql, int page, int pageSize);
    
    public int rowCount(String hql);
    
    public List<Object> sqlFindByPage(String sql);
    
    public int sqlRowCount(String sql);
    	
}