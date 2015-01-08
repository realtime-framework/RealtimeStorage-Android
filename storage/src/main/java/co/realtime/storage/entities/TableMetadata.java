package co.realtime.storage.entities;

import co.realtime.storage.StorageRef.StorageDataType;
import co.realtime.storage.StorageRef.StorageProvisionLoad;
import co.realtime.storage.StorageRef.StorageProvisionType;

public class TableMetadata {
	String applicationKey;
	String name;
	StorageProvisionType provisionType;
	StorageProvisionLoad provisionLoad;
	Integer throughputRead;
	Integer throughputWrite;
	Long creationDate;
	Long updateDate;
	Boolean isActive;
	String primaryKeyName;
	StorageDataType primaryKeyType;
	String secondaryKeyName;
	StorageDataType secondaryKeyType;
	String status;
	Long size;
	Long itemCount;	
	
	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public StorageProvisionType getProvisionType() {
		return provisionType;
	}

	public void setProvisionType(StorageProvisionType provisionType) {
		this.provisionType = provisionType;
	}

	public StorageProvisionLoad getProvisionLoad() {
		return provisionLoad;
	}

	public void setProvisionLoad(StorageProvisionLoad provisionLoad) {
		this.provisionLoad = provisionLoad;
	}

	public Integer getThroughputRead() {
		return throughputRead;
	}

	public void setThroughputRead(Integer throughputRead) {
		this.throughputRead = throughputRead;
	}

	public Integer getThroughputWrite() {
		return throughputWrite;
	}

	public void setThroughputWrite(Integer throughputWrite) {
		this.throughputWrite = throughputWrite;
	}

	public Long getCreationDate() {
		return creationDate;
	}

	public void setCreationDate(Long creationDate) {
		this.creationDate = creationDate;
	}

	public Long getUpdateDate() {
		return updateDate;
	}

	public void setUpdateDate(Long updateDate) {
		this.updateDate = updateDate;
	}

	public Boolean getIsActive() {
		return isActive;
	}

	public void setIsActive(Boolean isActive) {
		this.isActive = isActive;
	}

	public String getPrimaryKeyName() {
		return primaryKeyName;
	}

	public void setPrimaryKeyName(String primaryKeyName) {
		this.primaryKeyName = primaryKeyName;
	}

	public StorageDataType getPrimaryKeyType() {
		return primaryKeyType;
	}

	public void setPrimaryKeyType(StorageDataType primaryKeyType) {
		this.primaryKeyType = primaryKeyType;
	}

	public String getSecondaryKeyName() {
		return secondaryKeyName;
	}

	public void setSecondaryKeyName(String secondaryKeyName) {
		this.secondaryKeyName = secondaryKeyName;
	}

	public StorageDataType getSecondaryKeyType() {
		return secondaryKeyType;
	}

	public void setSecondaryKeyType(StorageDataType secondaryKeyType) {
		this.secondaryKeyType = secondaryKeyType;
	}

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}

	public Long getSize() {
		return size;
	}

	public void setSize(Long size) {
		this.size = size;
	}

	public Long getItemCount() {
		return itemCount;
	}

	public void setItemCount(Long itemCount) {
		this.itemCount = itemCount;
	}

	public String getApplicationKey() {
		return applicationKey;
	}

	public void setApplicationKey(String applicationKey) {
		this.applicationKey = applicationKey;
	}
}
