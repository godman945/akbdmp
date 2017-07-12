package com.pchome.akbdmp.mysql.db.dao.base;

import java.io.Serializable;
import java.lang.reflect.ParameterizedType;
import java.math.BigInteger;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.hibernate.Criteria;
import org.hibernate.FlushMode;
import org.hibernate.HibernateException;
import org.hibernate.Query;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.criterion.Projections;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.orm.hibernate4.HibernateCallback;
import org.springframework.orm.hibernate4.support.HibernateDaoSupport;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;


@Repository
@Transactional
public abstract class BaseDAO<T, PK extends Serializable> extends HibernateDaoSupport implements IBaseDAO<T, PK> {
    protected Log log = LogFactory.getLog(this.getClass());

    @Autowired
	 public void setSuperSessionFactory(SessionFactory sessionFactory){
    	super.setSessionFactory(sessionFactory);
	 } 
    
    private Class<T> clazz;

    @SuppressWarnings("unchecked")
    protected Class<T> getMyClass() {
        if (clazz == null) {
            clazz = (Class<T>) ((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[0];
        }
        return clazz;
    }

    public T get(Serializable id) {
    	return getHibernateTemplate().get(getMyClass(), id);
    }

    public long loadAllSize() {
        return getHibernateTemplate().loadAll(getMyClass()).size();
    }

    public List<T> loadAll() {
        return getHibernateTemplate().loadAll(getMyClass());
    }
    
    @SuppressWarnings("unchecked")
	public List<T> findHql(String hql,Object[] value) {
        return (List<T>) getHibernateTemplate().find(hql.toString(),value);
    }

    @SuppressWarnings("unchecked")
    public List<T> loadAll(int firstResult, int maxResults) {
        return getHibernateTemplate().getSessionFactory().getCurrentSession().createCriteria(getMyClass()).setFirstResult(firstResult).setMaxResults(maxResults).list();
    }

    @SuppressWarnings("unchecked")
    public PK save(T entity) {
    	getHibernateTemplate().getSessionFactory().getCurrentSession().setFlushMode(FlushMode.AUTO);
    	getHibernateTemplate().save(entity);
    	getHibernateTemplate().flush();
    	return (PK) getHibernateTemplate().save(entity);
    }

    
    public void update(T entity) {
    	
    	getHibernateTemplate().getSessionFactory().getCurrentSession().setFlushMode(FlushMode.AUTO);
        getHibernateTemplate().update(entity);
        getHibernateTemplate().flush();
        getHibernateTemplate().clear();
    }

    public void saveOrUpdate(T entity) {
    	getHibernateTemplate().getSessionFactory().getCurrentSession().setFlushMode(FlushMode.AUTO);
    	getHibernateTemplate().saveOrUpdate(entity);
    	getHibernateTemplate().flush();
    	getHibernateTemplate().clear();
    }

    public void delete(T entity) {
    	getHibernateTemplate().getSessionFactory().getCurrentSession().setFlushMode(FlushMode.AUTO);
        getHibernateTemplate().delete(entity);
        getHibernateTemplate().flush();
        getHibernateTemplate().clear();
    }

    protected int nextVal(final String sql) {
        BigInteger nextval = getHibernateTemplate().execute(
            new HibernateCallback<BigInteger>() {
                public BigInteger doInHibernate(Session session) throws HibernateException {
                    return (BigInteger) session.createSQLQuery(sql).uniqueResult();
                }
            });
        return nextval.intValue();
    }
    
    public List<T> findByPage(String hql, int page, int pageSize) {
		Query query = getHibernateTemplate().getSessionFactory().getCurrentSession().createQuery(hql);
		int firstResult = (page - 1) * pageSize;
		query.setFirstResult(firstResult);
		query.setMaxResults(pageSize);
		return query.list();
    }
    
//    public int rowCount() {
//		String generatedCountHql = " from AdmCategoryAudienceAnalyze where 1=1 ";
//		Query countQuery = getHibernateTemplate().getSessionFactory().getCurrentSession().createQuery(generatedCountHql);
//		int count;
//		 count=((Long) countQuery.uniqueResult()).intValue();
//		 System.out.println(count);
//		 return count;
//	}
    
    public int rowCount(String hql) {
  		Query query = getHibernateTemplate().getSessionFactory().getCurrentSession().createQuery(hql);
  		return query.list().size();
    }
}
