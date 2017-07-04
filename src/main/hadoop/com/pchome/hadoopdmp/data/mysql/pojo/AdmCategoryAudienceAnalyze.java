package com.pchome.hadoopdmp.data.mysql.pojo;
// Generated 2017/7/4 �W�� 09:43:47 by Hibernate Tools 3.4.0.CR1

import java.util.Date;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import static javax.persistence.GenerationType.IDENTITY;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;

/**
 * AdmCategoryAudienceAnalyze generated by hbm2java
 */
@Entity
@Table(name = "adm_category_audience_analyze", catalog = "dmp")
public class AdmCategoryAudienceAnalyze implements java.io.Serializable {

	private Integer id;
	private Date recordDate;
	private String keyId;
	private String keyType;
	private String userType;
	private String source;
	private int keyCount;
	private Date createDate;
	private Date updateDate;

	public AdmCategoryAudienceAnalyze() {
	}

	public AdmCategoryAudienceAnalyze(Date recordDate, String keyId, String keyType, String userType, String source,
			int keyCount, Date createDate, Date updateDate) {
		this.recordDate = recordDate;
		this.keyId = keyId;
		this.keyType = keyType;
		this.userType = userType;
		this.source = source;
		this.keyCount = keyCount;
		this.createDate = createDate;
		this.updateDate = updateDate;
	}

	@Id
	@GeneratedValue(strategy = IDENTITY)

	@Column(name = "id", unique = true, nullable = false)
	public Integer getId() {
		return this.id;
	}

	public void setId(Integer id) {
		this.id = id;
	}

	@Temporal(TemporalType.DATE)
	@Column(name = "record_date", nullable = false, length = 10)
	public Date getRecordDate() {
		return this.recordDate;
	}

	public void setRecordDate(Date recordDate) {
		this.recordDate = recordDate;
	}

	@Column(name = "key_id", nullable = false, length = 20)
	public String getKeyId() {
		return this.keyId;
	}

	public void setKeyId(String keyId) {
		this.keyId = keyId;
	}

	@Column(name = "key_type", nullable = false, length = 5)
	public String getKeyType() {
		return this.keyType;
	}

	public void setKeyType(String keyType) {
		this.keyType = keyType;
	}

	@Column(name = "user_type", nullable = false, length = 10)
	public String getUserType() {
		return this.userType;
	}

	public void setUserType(String userType) {
		this.userType = userType;
	}

	@Column(name = "source", nullable = false, length = 10)
	public String getSource() {
		return this.source;
	}

	public void setSource(String source) {
		this.source = source;
	}

	@Column(name = "key_count", nullable = false)
	public int getKeyCount() {
		return this.keyCount;
	}

	public void setKeyCount(int keyCount) {
		this.keyCount = keyCount;
	}

	@Temporal(TemporalType.TIMESTAMP)
	@Column(name = "create_date", nullable = false, length = 19)
	public Date getCreateDate() {
		return this.createDate;
	}

	public void setCreateDate(Date createDate) {
		this.createDate = createDate;
	}

	@Temporal(TemporalType.TIMESTAMP)
	@Column(name = "update_date", nullable = false, length = 19)
	public Date getUpdateDate() {
		return this.updateDate;
	}

	public void setUpdateDate(Date updateDate) {
		this.updateDate = updateDate;
	}

}
