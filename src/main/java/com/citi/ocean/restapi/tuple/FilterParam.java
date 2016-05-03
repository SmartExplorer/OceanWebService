package com.citi.ocean.restapi.tuple;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Optional;

import com.citi.ocean.restapi.util.ExceptionUtil;

public class FilterParam {
	
	private String key;
	private String[] vals;
	
	public FilterParam(String key, String[] vals){
		this.key = key;
		this.vals = vals;
	}

	public String getKey() {
		return key;
	}

	public String[] getVals() {
		return vals;
	}
	
	public static class DateFilterParam extends FilterParam{
		
		private Optional<LocalDate> startDate;
		private Optional<LocalDate> endDate;
		
		public DateFilterParam(String key, String[] vals){
			super(key, vals);
			DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd");
			if(!vals[0].equals("")){
				try{
					startDate = Optional.of(LocalDate.parse(vals[0], formatter));
				}catch(DateTimeParseException ex){
					throw new IllegalArgumentException(key + ExceptionUtil.EXCEPTION_MSG_ILLEGAL_DATATYPE, ex);
				}
			}else{
				startDate = Optional.ofNullable(null);
			}
			if(vals.length > 1){
				if(!vals[1].equals("")){
					try{
						endDate = Optional.of(LocalDate.parse(vals[1], formatter));
					}catch(DateTimeParseException ex){
						throw new IllegalArgumentException(key + ExceptionUtil.EXCEPTION_MSG_ILLEGAL_DATATYPE, ex);
					}
				}else{
					endDate = Optional.ofNullable(null);
				}
			}
		}

		public Optional<LocalDate> getStartDate() {
			return startDate;
		}

		public Optional<LocalDate> getEndDate() {
			return endDate;
		}
	}
}
