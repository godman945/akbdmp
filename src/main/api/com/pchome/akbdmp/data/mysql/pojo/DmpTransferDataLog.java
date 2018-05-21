package com.pchome.akbdmp.data.mysql.pojo;
// Generated 2018/5/20 �U�� 01:29:53 by Hibernate Tools 3.4.0.CR1

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import static javax.persistence.GenerationType.IDENTITY;
import javax.persistence.Id;
import javax.persistence.Table;

/**
 * DmpTransferDataLog generated by hbm2java
 */
@Entity
@Table(name = "dmp_transfer_data_log")
public class DmpTransferDataLog implements java.io.Serializable {

	private Integer seq;
	private String recordDate;
	private String status;

	public DmpTransferDataLog() {
	}

	public DmpTransferDataLog(String recordDate, String status) {
		this.recordDate = recordDate;
		this.status = status;
	}

	@Id
	@GeneratedValue(strategy = IDENTITY)

	@Column(name = "seq", unique = true, nullable = false)
	public Integer getSeq() {
		return this.seq;
	}

	public void setSeq(Integer seq) {
		this.seq = seq;
	}

	@Column(name = "record_date", nullable = false, length = 10)
	public String getRecordDate() {
		return this.recordDate;
	}

	public void setRecordDate(String recordDate) {
		this.recordDate = recordDate;
	}

	@Column(name = "status", nullable = false, length = 25)
	public String getStatus() {
		return this.status;
	}

	public void setStatus(String status) {
		this.status = status;
	}

}
