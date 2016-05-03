package com.citi.ocean.restapi.datasource.providers;

import java.io.IOException;
import org.apache.log4j.Logger;
import kx.c;
import kx.c.KException;
import io.vertx.core.eventbus.EventBus;


public class KDBQueryExecutor extends QueryExecutor {
	
	private static final Logger log = Logger.getLogger(KDBQueryExecutor.class);
 
	private final String host;
	private final long port;
	private final String user;
	private final String password;
	
	private c conn;
	
	public KDBQueryExecutor(EventBus outputBus, String tableName, String host, int port, String user, String password) throws KException, IOException {
		super(outputBus);
		this.host = host;
		this.port = port;
		this.user = user;
		this.password = password;
		this.conn = new c(host, port, user + ":" + password);
	}
	
	@Override
	public void executeQuery(String topic, String tableName, String sourceQuery) {
/*		TableSchema schema = ConfigUtil.getTableSchemaRegistry().getTableSchema(tableName);
		try {
			Object resObj = conn.k(sourceQuery);
			if (resObj instanceof Flip) {
				Flip res = (Flip)resObj;
				Buffer buf = Buffer.Buffer();
				foreach( .... ) {
					if (currentBufferSize >= batch size) {
						outputBus.send(topic, buf)
					}
					buf.appendInt(r)
					buf.appendString(r)
					buf.appendLong(r)
				}
			} else {
				throw new IOException("KDB return types does not present table");
			}
		} catch (KException | IOException e) {
			publishError(e.getMessage());
		}*/
		// Produce output results to outputBus using topic specified
		// Serialization should be done in accordance with schema provided at Config Util TableSchemaRegistry by tableName   
	}
	
	@Override
	public void close() {
		try {
			conn.close();
		} catch (IOException e) {
			log.error("Unable to close KDB connection: ", e);
		}
	}
}
