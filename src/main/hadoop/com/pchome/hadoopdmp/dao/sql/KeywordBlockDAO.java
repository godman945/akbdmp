package com.pchome.hadoopdmp.dao.sql;



public class KeywordBlockDAO extends BaseDAO implements IKeywordBlockDAO{
	private static IKeywordBlockDAO instance = new KeywordBlockDAO();
	private KeywordBlockDAO() {};
	
    public static IKeywordBlockDAO getInstance() {
        return instance;
    }
	
    @Override
	public boolean selectKeywordBlockByKeyword(String keyword){
		Object[] args = new Object[]{new String(keyword)};
		String sql = "SELECT keyword FROM keyword_block WHERE keyword = ?";
		return this.executeQuery(sql, args);

	}
	
}

	
	
