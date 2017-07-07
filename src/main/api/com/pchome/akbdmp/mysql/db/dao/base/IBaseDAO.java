package com.pchome.akbdmp.mysql.db.dao.base;

import java.io.Serializable;
import java.util.List;

import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

@Repository
@Transactional
public interface IBaseDAO<T, PK extends Serializable> {
    public T get(Serializable id);

    public long loadAllSize();

    public List<T> loadAll();
    
    public List<T> findHql(String hql,Object[] value);

    public List<T> loadAll(int firstResult, int maxResults);

    public PK save(T entity);

    public void update(T entity);

    public void saveOrUpdate(T entity);

    public void delete(T entity);
}