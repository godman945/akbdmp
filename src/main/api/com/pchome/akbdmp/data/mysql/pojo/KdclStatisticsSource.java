package com.pchome.akbdmp.data.mysql.pojo;
// Generated 2018/5/20 �U�� 01:29:53 by Hibernate Tools 3.4.0.CR1

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
 * KdclStatisticsSource generated by hbm2java
 */
@Entity
@Table(name = "kdcl_statistics_source")
public class KdclStatisticsSource implements java.io.Serializable {

	private Integer id;
	private String idType;
	private String serviceType;
	private String behavior;
	private String classify;
	private int counter;
	private String recordDate;
	private Date updateDate;
	private Date createDate;

	public KdclStatisticsSource() {
	}

	public KdclStatisticsSource(String idType, String serviceType, String behavior, String classify, int counter,
			String recordDate, Date updateDate, Date createDate) {
		this.idType = idType;
		this.serviceType = serviceType;
		this.behavior = behavior;
		this.classify = classify;
		this.counter = counter;
		this.recordDate = recordDate;
		this.updateDate = updateDate;
		this.createDate = createDate;
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

	@Column(name = "id_type", nullable = false, length = 50)
	public String getIdType() {
		return this.idType;
	}

	public void setIdType(String idType) {
		this.idType = idType;
	}

	@Column(name = "service_type", nullable = false, length = 20)
	public String getServiceType() {
		return this.serviceType;
	}

	public void setServiceType(String serviceType) {
		this.serviceType = serviceType;
	}

	@Column(name = "behavior", nullable = false, length = 20)
	public String getBehavior() {
		return this.behavior;
	}

	public void setBehavior(String behavior) {
		this.behavior = behavior;
	}

	@Column(name = "classify", nullable = false, length = 1)
	public String getClassify() {
		return this.classify;
	}

	public void setClassify(String classify) {
		this.classify = classify;
	}

	@Column(name = "counter", nullable = false)
	public int getCounter() {
		return this.counter;
	}

	public void setCounter(int counter) {
		this.counter = counter;
	}

	@Column(name = "record_date", nullable = false, length = 10)
	public String getRecordDate() {
		return this.recordDate;
	}

	public void setRecordDate(String recordDate) {
		this.recordDate = recordDate;
	}

	@Temporal(TemporalType.TIMESTAMP)
	@Column(name = "update_date", nullable = false, length = 0)
	public Date getUpdateDate() {
		return this.updateDate;
	}

	public void setUpdateDate(Date updateDate) {
		this.updateDate = updateDate;
	}

	@Temporal(TemporalType.TIMESTAMP)
	@Column(name = "create_date", nullable = false, length = 0)
	public Date getCreateDate() {
		return this.createDate;
	}

	public void setCreateDate(Date createDate) {
		this.createDate = createDate;
	}

}
