package com.citi.ocean.restapi.datasource.providers;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Optional;

import org.apache.log4j.Logger;

import com.citi.ocean.restapi.datasource.base.TableSchema;
import com.citi.ocean.restapi.util.ConfigUtil;
import com.citi.ocean.restapi.util.MonitorUtil;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.MessageProducer;

public class NetezzaQueryExecutor extends QueryExecutor {

	private static final Logger log = Logger.getLogger(NetezzaQueryExecutor.class);

	private final String url;
	private final String user;
	private final String password;
	
	private Connection conn;
	
	//TODO hardcode here!!
	int batchSize = 10, maxQueueSize = 2;	
	
	public NetezzaQueryExecutor(EventBus outputBus,								 
								String url, 
								String user,
								String password) {
		super(outputBus);
		this.url = url;
		this.user = user;
		this.password = password;		
		
		init();
	}


	private void init() {
		try {
			Class.forName("org.netezza.Driver");
			log.debug("connecting..");
			conn = DriverManager.getConnection(this.url, this.user, this.password);
			log.info("Successfully connected to Netezza.");
		} catch (Exception e) {
			log.error(e.getMessage());
		}
	}
	
	
	public void executeCountQuery(String replyTopic, String countQuery, String query) {
		Statement st = null;
		ResultSet rs = null;
		
		try {
			if (conn.isClosed()) {
				init();
			}			
			st = conn.createStatement();
			rs = st.executeQuery(countQuery);
			
			if (rs.next()) {
				outputBus.send(replyTopic, Buffer.buffer().appendLong(rs.getLong(1)), 
						new DeliveryOptions().addHeader(QueryExecutor.COUNT_HEADER, query));
			}
		} catch (Exception e) {
			log.error(e.getMessage());
		} finally {
			try {
				if (rs != null) {
					rs.close();
				}
				if (st != null) {
					st.close();
				}
			} catch (SQLException e1) {
				log.error(e1.getMessage());
			}
		}		
		
	}
	
	@Override
	public void executeQuery(String replyTopic, String tableName, String sourceQuery) {
		// TODO Auto-generated method stub
		// Produce output results to outputBus using topic specified
		// Serialization should be done in accordance with schema provided at
		// Config Util TableSchemaRegistry by tableName
		
		MessageProducer<Buffer> sender = outputBus.sender(replyTopic);
		
//		MessageProducer<JsonArray> senderTest = outputBus.sender("test");
		
		Statement st = null;
		ResultSet rs = null;
		
		TableSchema schema = ConfigUtil.getTableSchemaRegistry().getTableSchema(tableName);
		

		sender.setWriteQueueMaxSize(maxQueueSize);

		int recCount = 0;
		long startTime = System.currentTimeMillis();

		try {
			if (conn.isClosed()) {
				init();
			}
			st = conn.createStatement();
			rs = st.executeQuery(sourceQuery);
			log.debug("Executed query: " + sourceQuery);
			
//			JsonArray bufferJson = new JsonArray();
			Buffer sendBuffer = Buffer.buffer();
			while(true){
				Boolean hasNext = rs.next();
				if(recCount >= batchSize || (!hasNext && recCount > 0)){
					if(sender.writeQueueFull()){
						log.error("JDBC Write queue full");
						break;
					}
					
//					senderTest.write(bufferJson);
					sender.write(sendBuffer);
					log.debug("Send data to buffer on " + replyTopic);
					
//					bufferJson = new JsonArray();
					sendBuffer = Buffer.buffer();
					
					recCount = 0;
				}

				if(!hasNext){
					break;
				}
//				JsonArray rec = getMapRepresentation_json(rs, schema);
				Buffer raw = getMapRepresentation(rs, schema);
//				bufferJson.add(rec);
				sendBuffer.appendBuffer(raw);
				//log.debug("Append raw count " + recCount);
				
				recCount++;
			}				
		} catch (Exception e) {
			log.error(e.getMessage());
			this.publishError(replyTopic, e.getMessage());
		} finally {
			this.publishEOR(replyTopic, startTime);
			try {
				if (rs != null) {
					rs.close();
				}
				if (st != null) {
					st.close();
				}
			} catch (SQLException e1) {
				log.error(e1.getMessage());
			}
		}
	}

	@Override
	public void close() {
		if (conn != null) {
			try {
				conn.close();
				log.debug("disconnected");
			} catch (SQLException e) {
				log.error(e.getMessage());
			}
		}
	}

	
	private Buffer getMapRepresentation(ResultSet rs, TableSchema schema) {
		ResultSetMetaData rsmd;
		Buffer currentBuffer = Buffer.buffer();
		
		int colCount = 0;
		try {
			rsmd = rs.getMetaData();
			colCount = rsmd.getColumnCount();

			for (int i=1; i<=colCount ; i++) {
				String fieldType = null, fieldName = null;
				try {
					fieldName = rsmd.getColumnName(i);
				} catch (SQLException e1) {
					log.error(e1.getMessage());
				}
				
				Object value = null;
				fieldType = schema.getColumnType(fieldName);
				if (fieldType == null) {
					switch (fieldName.toUpperCase()) {
					case "SUM":
					case "MIN":
					case "MAX":
						fieldType = rsmd.getColumnTypeName(i);
						break;
					case "COUNT":
						fieldType = "java.lang.Long";
						break;
					case "DATE":
						fieldType = "java.time.LocalDate";
						break;
					default:
						fieldType = rsmd.getColumnTypeName(i);
						log.warn("unrecognizable fieldName" + fieldName);
						break;
					}
				}
				try {
					switch (fieldType) {
						case "java.lang.Long":
						case "BIGINT":
							value = Optional.ofNullable(rs.getLong(fieldName)).orElse((long) 0);
							currentBuffer.appendLong((long) value);
							break;
							
						case "java.lang.String":
						case "VARCHAR":
							value = ((String) Optional.ofNullable(rs.getString(fieldName)).orElse("")).trim();
							currentBuffer.appendLong(((String) value).length());
							currentBuffer.appendString((String) value);
							break;
							
						case "java.time.LocalDate":
						case "DATE":
							Date date = rs.getDate(fieldName);
							if (date != null) {
								value = date.getTime();
							}
							else {
								value = 0L;
							}
							currentBuffer.appendLong((long) value);
							break;
							
						case "java.time.Instant":
						case "TIMESTAMP":
							Timestamp ts = rs.getTimestamp(fieldName);
							if (ts != null) {
								value = ts.getTime();
							}
							else {
								value = 0L;
							}
							currentBuffer.appendLong((long)value);
							break;
							
						case "java.lang.Double":
						case "NUMERIC":
							value = Optional.ofNullable(rs.getDouble(fieldName)).orElse(0.0);
							currentBuffer.appendDouble((double) value);
							break;
							
						case "char":
						case "CHAR":
							String st = rs.getString(fieldName);
							if (st != null) {
								value = st.getBytes();
								currentBuffer.appendBytes((byte[]) value);
							}
							else {
								value = new byte[1];
								currentBuffer.appendBytes((byte[]) value);
							}
							break;
							
						default:
							log.warn("unrecognizable fieldType in TableSchema for field" + fieldName + " type " + fieldType);
							break;
					}
				} catch (SQLException e) {
					log.error(e.getMessage());
				}			
				catch (Exception e1) {
					log.error(e1.getMessage());
				}
			}
		} catch (SQLException e1) {
			log.error(e1.getMessage());
		}
		return currentBuffer;
	}
}