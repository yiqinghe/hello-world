package com.test.common;

import backtype.storm.spout.Scheme;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.util.List;

public class MessageScheme implements Scheme {
    private static final Logger log = LoggerFactory.getLogger(MessageScheme.class);

    /* (non-Javadoc)
     * @see backtype.storm.spout.Scheme#deserialize(byte[])
     */
    public List<Object> deserialize(byte[] ser) {
        try {
            String msg = new String(ser, "UTF-8");
            List<Object> values = new Values(msg);
            log.info("deserialize2"+values.get(0));
            return values;
        } catch (UnsupportedEncodingException e) {  
         
        }
        return null;
    }
    
    
    /* (non-Javadoc)
     * @see backtype.storm.spout.Scheme#getOutputFields()
     */
    public Fields getOutputFields() {
        return new Fields(Constant.ubaseFiled);
    }  
}