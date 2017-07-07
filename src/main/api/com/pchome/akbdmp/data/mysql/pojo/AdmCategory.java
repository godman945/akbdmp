package com.pchome.akbdmp.data.mysql.pojo;
// Generated 2017/7/4 �U�� 05:58:06 by Hibernate Tools 3.4.0.CR1

import java.util.Date;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import static javax.persistence.GenerationType.IDENTITY;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.Table;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;

/**
 * AdmCategory generated by hbm2java
 */
@Entity
@Table(name = "adm_category", catalog = "dmp")
public class AdmCategory implements java.io.Serializable {

	private Integer id;
	private AdmCategoryGroup admCategoryGroup;
	private String adClass;
	private String adClassName;
	private Date createDate;
	private Date updateDate;

	public AdmCategory() {
	}

	public AdmCategory(AdmCategoryGroup admCategoryGroup, String adClass, String adClassName, Date createDate,
			Date updateDate) {
		this.admCategoryGroup = admCategoryGroup;
		this.adClass = adClass;
		this.adClassName = adClassName;
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

	@ManyToOne(fetch = FetchType.LAZY)
	@JoinColumn(name = "adm_category_group_group_id", nullable = false)
	public AdmCategoryGroup getAdmCategoryGroup() {
		return this.admCategoryGroup;
	}

	public void setAdmCategoryGroup(AdmCategoryGroup admCategoryGroup) {
		this.admCategoryGroup = admCategoryGroup;
	}

	@Column(name = "ad_class", nullable = false, length = 16)
	public String getAdClass() {
		return this.adClass;
	}

	public void setAdClass(String adClass) {
		this.adClass = adClass;
	}

	@Column(name = "ad_class_name", nullable = false, length = 20)
	public String getAdClassName() {
		return this.adClassName;
	}

	public void setAdClassName(String adClassName) {
		this.adClassName = adClassName;
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
