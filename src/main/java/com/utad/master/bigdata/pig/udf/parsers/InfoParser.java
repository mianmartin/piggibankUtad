package com.utad.master.bigdata.pig.udf.parsers;

import java.io.IOException;
import java.util.Date;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;

//DEvuelve una tupla 

public class InfoParser extends EvalFunc<Tuple> {
	// El metodo principal.
	// Se ejecuta para cada llamada a esta UDF

	public Tuple exec(Tuple input) throws IOException {
		if (null == input || input.size() != 1) {
			return null;
		}

		String line = (String) input.get(0);
		try {
			TupleFactory tf = TupleFactory.getInstance();
			Tuple t = tf.newTuple();

			t.append(getOS());
			t.append(getArquitectura());
			t.append(getDate());

			// La tupla devuelta tiene ahora 3 elementos todos String
			// The tuple we are returning now has 3 elements, all strings.
			// In order, they are the HTTP method, the IP address, and the date.

			return t;
		} catch (Exception e) {
			// Any problems? Just return null and this one doesn't get
			// 'generated' by pig
			return null;
		}
	}

	public Schema outputSchema(Schema input) {
		try {
			Schema s = new Schema();

			s.add(new Schema.FieldSchema("Sistema", DataType.CHARARRAY));
			s.add(new Schema.FieldSchema("arquirectura", DataType.CHARARRAY));
			s.add(new Schema.FieldSchema("fecha", DataType.CHARARRAY));

			return s;
		} catch (Exception e) {
			// Any problems? Just return null...there probably won't be any
			// problems though.
			return null;
		}
	}

	public String getDate() {
		Date fecha = new Date();
		return fecha.toGMTString();
	}

	public String getOS() {
		return System.getProperty("os.name");
	}

	public String getArquitectura() {
		return System.getProperty("os.arch");
	}

}