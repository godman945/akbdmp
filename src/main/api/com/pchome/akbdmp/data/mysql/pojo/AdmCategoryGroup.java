package com.pchome.akbdmp.data.mysql.pojo;
// Generated 2018/5/20 �U�� 01:29:53 by Hibernate Tools 3.4.0.CR1

import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.Id;
import javax.persistence.OneToMany;
import javax.persistence.Table;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;

/**
 * AdmCategoryGroup generated by hbm2java
 */
@Entity
@Table(name = "adm_category_group")
public class AdmCategoryGroup implements java.io.Serializable {

	private String groupId;
	private String groupName;
	private Date createDate;
	private Date updateDate;
	private Set<AdmCategory> admCategories = new HashSet<AdmCategory>(0);

	public AdmCategoryGroup() {
	}

	public AdmCategoryGroup(String groupId, String groupName, Date createDate, Date updateDate) {
		this.groupId = groupId;
		this.groupName = groupName;
		this.createDate = createDate;
		this.updateDate = updateDate;
	}

	public AdmCategoryGroup(String groupId, String groupName, Date createDate, Date updateDate,
			Set<AdmCategory> admCategories) {
		this.groupId = groupId;
		this.groupName = groupName;
		this.createDate = createDate;
		this.updateDate = updateDate;
		this.admCategories = admCategories;
	}

	@Id

	@Column(name = "group_id", unique = true, nullable = false, length = 16)
	public String getGroupId() {
		return this.groupId;
	}

	public void setGroupId(String groupId) {
		this.groupId = groupId;
	}

	@Column(name = "group_name", nullable = false, length = 20)
	public String getGroupName() {
		return this.groupName;
	}

	public void setGroupName(String groupName) {
		this.groupName = groupName;
	}

	@Temporal(TemporalType.TIMESTAMP)
	@Column(name = "create_date", nullable = false, length = 0)
	public Date getCreateDate() {
		return this.createDate;
	}

	public void setCreateDate(Date createDate) {
		this.createDate = createDate;
	}

	@Temporal(TemporalType.TIMESTAMP)
	@Column(name = "update_date", nullable = false, length = 0)
	public Date getUpdateDate() {
		return this.updateDate;
	}

	public void setUpdateDate(Date updateDate) {
		this.updateDate = updateDate;
	}

	@OneToMany(fetch = FetchType.LAZY, mappedBy = "admCategoryGroup")
	public Set<AdmCategory> getAdmCategories() {
		return this.admCategories;
	}

	public void setAdmCategories(Set<AdmCategory> admCategories) {
		this.admCategories = admCategories;
	}

}
