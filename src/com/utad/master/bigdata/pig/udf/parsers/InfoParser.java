package com.utad.master.bigdata.pig.udf.parsers;

import java.io.IOException;
import java.util.Date;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;

//Devuelve una tupla nueva tupla con un primer campo
//de fecha de cada una de las que lee y añadiendo datos
//del sistema a cada una de ellas

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
			if (line != null) {
				t.append(line.substring(9, 29));

			} else {
				t.append("Sin fecha en log");

			}
			t.append(getOS());
			t.append(getArquitectura());
			t.append(getDate());
			// La tupla devuelta tiene ahora 4 elementos todos String

			return t;
		} catch (Exception e) {
			// devolvemos null para que no pare

			return null;
		}
	}


	// return new Schema(new
	// Schema.FieldSchema(getSchemaName(this.getClass().getName().toLowerCase(),
	// input),tupleSchema, DataType.TUPLE));

	public Schema outputSchema(Schema input) {
		try {
			Schema s = new Schema();
			s.add(new Schema.FieldSchema("fechaLog", DataType.CHARARRAY));

			s.add(new Schema.FieldSchema("Sistema", DataType.CHARARRAY));
			s.add(new Schema.FieldSchema("arquirectura", DataType.CHARARRAY));
			s.add(new Schema.FieldSchema("fecha", DataType.CHARARRAY));


			// Cone esto conseguimos este esquema

			return new Schema(new Schema.FieldSchema(getSchemaName(
					"infoParser", input), s,
					DataType.TUPLE));
			// Asi tenemos este esqeuma
			// log_udf: {infoParser_line_9::fechaLog:
			// chararray,infoParser_line_9::Sistema:
			// chararray,infoParser_line_9::arquirectura:
			// chararray,infoParser_line_9::fecha: chararray}

		} catch (Exception e) {
			// devolvemos null para que no pare
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