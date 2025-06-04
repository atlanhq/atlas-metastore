package com.orztip.sonyflake;

public class IdGeneratorProperties {
	
	public static final int LENGTH_TIME_BIT_SLOT = 39;
	
    /**
     *-1 ^  (-1 << self::LENGTH_TIMESTAMP_IN_10MS)
     * @var int
     */
	public static final long MAX_NUMBER_TIME_BIT_SLOT = 549755813887L;
	
	public static final int LENGTH_OTHER_BIT_SLOT = 24;

	private volatile int lengthSequenceBit = 8;
	
	private volatile long maxNumberSequenceBit = 255;

	private volatile int lengthMachineIdBit = 16;
	
	private volatile long maxNumberMachineIdBit = 65535;

	/**
	 * 起始值（10ms）
	 * 2014-09-01 00:00:00 +0000 UTC的unix值
	 */
	private volatile long startTimestampIn10ms = 1409529600;
	
	private volatile long machineId = 0;
	
	private volatile boolean waitForNextTimeBitSlotIfUnusual = true;
	
	private volatile boolean lock = false;
	
	public IdGeneratorProperties() {
		
	}
	
	public IdGeneratorProperties(int lengthSequenceBit, int lengthMachineIdBit) {
		this.setLengthBit(lengthSequenceBit, lengthMachineIdBit);
	}
	
	private void setLengthBit(int lengthSequenceBit, int lengthMachineIdBit) {
		
		if(lengthSequenceBit <= 0) {
			throw new IllegalArgumentException("Param lengthSequenceBit should be greater than 0");
		}
		
		if(lengthMachineIdBit <= 0) {
			throw new IllegalArgumentException("Param lengthMachineIdBit should be greater than 0");
		}
		
		if(lengthSequenceBit + lengthMachineIdBit  != LENGTH_OTHER_BIT_SLOT) {
			throw new IllegalArgumentException("Param lengthSequenceBit + lengthMachineIdBit  should be equal to " + LENGTH_OTHER_BIT_SLOT);
		}
		
		this.lengthSequenceBit = lengthSequenceBit;
		this.maxNumberSequenceBit =  -1 ^  (-1 << lengthSequenceBit);
		
		this.lengthMachineIdBit = lengthMachineIdBit;
		this.maxNumberMachineIdBit =  -1 ^  (-1 << lengthMachineIdBit);
		
	}

	public void setMachineId(long machindId) {
		
		if(this.lock) {
			throw new RuntimeException("This prop has been in used, can not set any more");
		}
		
		if(machindId < 0) {
			throw new IllegalArgumentException("machine id must greater than or equal to 0");
		}
		
		if(machindId > this.maxNumberMachineIdBit) {
			throw new IllegalArgumentException("machine id must lower than " + this.maxNumberMachineIdBit);
		}
		
		this.machineId = machindId;
	}
	
	
	public long getMachineId() {
		return this.machineId;
	}
	
	
	public void setStartTimestampByMilsec(long milsec) {

		if(this.lock) {
			throw new RuntimeException("This prop has been in used, can not set any more");
		}
		
		this.startTimestampIn10ms = milsec / 10L;
	}
	
	public void setStartTimestampByUnixTimestamp(long sec) {

		if(this.lock) {
			throw new RuntimeException("This prop has been in used, can not set any more");
		}
		
		this.startTimestampIn10ms = sec * 100L;
		
	}
	
	public long getStartTimestampIn10ms() {
		return this.startTimestampIn10ms;
	}
	
	public int[] getBitAllocationConfig() {
		int[] bitAllocation = {
				LENGTH_TIME_BIT_SLOT, 
				this.lengthSequenceBit, 
				this.lengthMachineIdBit
		};
		return bitAllocation;
	}
	
	public long[] getBitAllocationMaxNumber() {
		long[] bitAllocationMaxNumber = {
				MAX_NUMBER_TIME_BIT_SLOT, 
				this.maxNumberSequenceBit, 
				this.maxNumberMachineIdBit
		};
		return bitAllocationMaxNumber;
	}
	
	public boolean getWaitForNextTimeBitSlotIfUnusual() {
		return this.waitForNextTimeBitSlotIfUnusual;
	}
	
	public void setWaitForNextTimeBitSlotIfUnusual(boolean val) {

		if(this.lock) {
			throw new RuntimeException("This prop has been in used, can not set any more");
		}
		
		this.waitForNextTimeBitSlotIfUnusual = val;
	}
	
	public void enableLock() {
		this.lock = true;
	}
	
}
