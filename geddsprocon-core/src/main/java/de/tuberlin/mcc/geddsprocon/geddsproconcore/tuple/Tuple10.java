/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// --------------------------------------------------------------
//  THIS IS A GENERATED SOURCE FILE. DO NOT EDIT!
//  GENERATED FROM org.apache.flink.api.java.tuple.TupleGenerator.
// --------------------------------------------------------------

package de.tuberlin.mcc.geddsprocon.geddsproconcore.tuple;

import org.apache.flink.annotation.Public;
import org.apache.flink.util.StringUtils;

/**
 * A tuple with 10 fields. Tuples are strongly typed; each field may be of a separate type.
 * The fields of the tuple can be accessed directly as public fields (f_0, f_1, ...) or via their position
 * through the {@link #getField(int)} method. The tuple field positions start at zero.
 *
 * <p>Tuples are mutable types, meaning that their fields can be re-assigned. This allows functions that work
 * with Tuples to reuse objects in order to reduce pressure on the garbage collector.</p>
 *
 * <p>Warning: If you subclass Tuple10, then be sure to either <ul>
 *  <li> not add any new fields, or </li>
 *  <li> make it a POJO, and always declare the element type of your DataStreams/DataSets to your descendant
 *       type. (That is, if you have a "class Foo extends Tuple10", then don't use instances of
 *       Foo in a DataStream&lt;Tuple10&gt; / DataSet&lt;Tuple10&gt;, but declare it as
 *       DataStream&lt;Foo&gt; / DataSet&lt;Foo&gt;.) </li>
 * </ul></p>
 * @see Tuple
 *
 * @param <T0> The type of field 0
 * @param <T1> The type of field 1
 * @param <T2> The type of field 2
 * @param <T3> The type of field 3
 * @param <T4> The type of field 4
 * @param <T5> The type of field 5
 * @param <T6> The type of field 6
 * @param <T7> The type of field 7
 * @param <T8> The type of field 8
 * @param <T9> The type of field 9
 */
@Public
public class Tuple10<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9> extends Tuple {

	private static final long serialVersionUID = 1L;

	/** Field 0 of the tuple. */
	public T0 f_0;
	/** Field 1 of the tuple. */
	public T1 f_1;
	/** Field 2 of the tuple. */
	public T2 f_2;
	/** Field 3 of the tuple. */
	public T3 f_3;
	/** Field 4 of the tuple. */
	public T4 f_4;
	/** Field 5 of the tuple. */
	public T5 f_5;
	/** Field 6 of the tuple. */
	public T6 f_6;
	/** Field 7 of the tuple. */
	public T7 f_7;
	/** Field 8 of the tuple. */
	public T8 f_8;
	/** Field 9 of the tuple. */
	public T9 f_9;

	/**
	 * Creates a new tuple where all fields are null.
	 */
	public Tuple10() {}

	/**
	 * Creates a new tuple and assigns the given values to the tuple's fields.
	 *
	 * @param value0 The value for field 0
	 * @param value1 The value for field 1
	 * @param value2 The value for field 2
	 * @param value3 The value for field 3
	 * @param value4 The value for field 4
	 * @param value5 The value for field 5
	 * @param value6 The value for field 6
	 * @param value7 The value for field 7
	 * @param value8 The value for field 8
	 * @param value9 The value for field 9
	 */
	public Tuple10(T0 value0, T1 value1, T2 value2, T3 value3, T4 value4, T5 value5, T6 value6, T7 value7, T8 value8, T9 value9) {
		this.f_0 = value0;
		this.f_1 = value1;
		this.f_2 = value2;
		this.f_3 = value3;
		this.f_4 = value4;
		this.f_5 = value5;
		this.f_6 = value6;
		this.f_7 = value7;
		this.f_8 = value8;
		this.f_9 = value9;
	}

	@Override
	public int getArity() {
		return 10;
	}

	@Override
	@SuppressWarnings("unchecked")
	public <T> T getField(int pos) {
		switch(pos) {
			case 0: return (T) this.f_0;
			case 1: return (T) this.f_1;
			case 2: return (T) this.f_2;
			case 3: return (T) this.f_3;
			case 4: return (T) this.f_4;
			case 5: return (T) this.f_5;
			case 6: return (T) this.f_6;
			case 7: return (T) this.f_7;
			case 8: return (T) this.f_8;
			case 9: return (T) this.f_9;
			default: throw new IndexOutOfBoundsException(String.valueOf(pos));
		}
	}

	@Override
	@SuppressWarnings("unchecked")
	public <T> void setField(T value, int pos) {
		switch(pos) {
			case 0:
				this.f_0 = (T0) value;
				break;
			case 1:
				this.f_1 = (T1) value;
				break;
			case 2:
				this.f_2 = (T2) value;
				break;
			case 3:
				this.f_3 = (T3) value;
				break;
			case 4:
				this.f_4 = (T4) value;
				break;
			case 5:
				this.f_5 = (T5) value;
				break;
			case 6:
				this.f_6 = (T6) value;
				break;
			case 7:
				this.f_7 = (T7) value;
				break;
			case 8:
				this.f_8 = (T8) value;
				break;
			case 9:
				this.f_9 = (T9) value;
				break;
			default: throw new IndexOutOfBoundsException(String.valueOf(pos));
		}
	}

	/**
	 * Sets new values to all fields of the tuple.
	 *
	 * @param value0 The value for field 0
	 * @param value1 The value for field 1
	 * @param value2 The value for field 2
	 * @param value3 The value for field 3
	 * @param value4 The value for field 4
	 * @param value5 The value for field 5
	 * @param value6 The value for field 6
	 * @param value7 The value for field 7
	 * @param value8 The value for field 8
	 * @param value9 The value for field 9
	 */
	public void setFields(T0 value0, T1 value1, T2 value2, T3 value3, T4 value4, T5 value5, T6 value6, T7 value7, T8 value8, T9 value9) {
		this.f_0 = value0;
		this.f_1 = value1;
		this.f_2 = value2;
		this.f_3 = value3;
		this.f_4 = value4;
		this.f_5 = value5;
		this.f_6 = value6;
		this.f_7 = value7;
		this.f_8 = value8;
		this.f_9 = value9;
	}


	// -------------------------------------------------------------------------------------------------
	// standard utilities
	// -------------------------------------------------------------------------------------------------

	/**
	 * Creates a string representation of the tuple in the form
	 * (f_0, f_1, f_2, f_3, f_4, f_5, f_6, f_7, f_8, f_9),
	 * where the individual fields are the value returned by calling {@link Object#toString} on that field.
	 * @return The string representation of the tuple.
	 */
	@Override
	public String toString() {
		return "(" + StringUtils.arrayAwareToString(this.f_0)
			+ "," + StringUtils.arrayAwareToString(this.f_1)
			+ "," + StringUtils.arrayAwareToString(this.f_2)
			+ "," + StringUtils.arrayAwareToString(this.f_3)
			+ "," + StringUtils.arrayAwareToString(this.f_4)
			+ "," + StringUtils.arrayAwareToString(this.f_5)
			+ "," + StringUtils.arrayAwareToString(this.f_6)
			+ "," + StringUtils.arrayAwareToString(this.f_7)
			+ "," + StringUtils.arrayAwareToString(this.f_8)
			+ "," + StringUtils.arrayAwareToString(this.f_9)
			+ ")";
	}

	/**
	 * Deep equality for tuples by calling equals() on the tuple members.
	 * @param o the object checked for equality
	 * @return true if this is equal to o.
	 */
	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (!(o instanceof Tuple10)) {
			return false;
		}
		@SuppressWarnings("rawtypes")
		Tuple10 tuple = (Tuple10) o;
		if (f_0 != null ? !f_0.equals(tuple.f_0) : tuple.f_0 != null) {
			return false;
		}
		if (f_1 != null ? !f_1.equals(tuple.f_1) : tuple.f_1 != null) {
			return false;
		}
		if (f_2 != null ? !f_2.equals(tuple.f_2) : tuple.f_2 != null) {
			return false;
		}
		if (f_3 != null ? !f_3.equals(tuple.f_3) : tuple.f_3 != null) {
			return false;
		}
		if (f_4 != null ? !f_4.equals(tuple.f_4) : tuple.f_4 != null) {
			return false;
		}
		if (f_5 != null ? !f_5.equals(tuple.f_5) : tuple.f_5 != null) {
			return false;
		}
		if (f_6 != null ? !f_6.equals(tuple.f_6) : tuple.f_6 != null) {
			return false;
		}
		if (f_7 != null ? !f_7.equals(tuple.f_7) : tuple.f_7 != null) {
			return false;
		}
		if (f_8 != null ? !f_8.equals(tuple.f_8) : tuple.f_8 != null) {
			return false;
		}
		if (f_9 != null ? !f_9.equals(tuple.f_9) : tuple.f_9 != null) {
			return false;
		}
		return true;
	}

	@Override
	public int hashCode() {
		int result = f_0 != null ? f_0.hashCode() : 0;
		result = 31 * result + (f_1 != null ? f_1.hashCode() : 0);
		result = 31 * result + (f_2 != null ? f_2.hashCode() : 0);
		result = 31 * result + (f_3 != null ? f_3.hashCode() : 0);
		result = 31 * result + (f_4 != null ? f_4.hashCode() : 0);
		result = 31 * result + (f_5 != null ? f_5.hashCode() : 0);
		result = 31 * result + (f_6 != null ? f_6.hashCode() : 0);
		result = 31 * result + (f_7 != null ? f_7.hashCode() : 0);
		result = 31 * result + (f_8 != null ? f_8.hashCode() : 0);
		result = 31 * result + (f_9 != null ? f_9.hashCode() : 0);
		return result;
	}

	/**
	* Shallow tuple copy.
	* @return A new Tuple with the same fields as this.
	*/
	@Override
	@SuppressWarnings("unchecked")
	public Tuple10<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9> copy() {
		return new Tuple10<>(this.f_0,
			this.f_1,
			this.f_2,
			this.f_3,
			this.f_4,
			this.f_5,
			this.f_6,
			this.f_7,
			this.f_8,
			this.f_9);
	}

	/**
	 * Creates a new tuple and assigns the given values to the tuple's fields.
	 * This is more convenient than using the constructor, because the compiler can
	 * infer the generic type arguments implicitly. For example:
	 * {@code Tuple3.of(n, x, s)}
	 * instead of
	 * {@code new Tuple3<Integer, Double, String>(n, x, s)}
	 */
	public static <T0, T1, T2, T3, T4, T5, T6, T7, T8, T9> Tuple10<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9> of(T0 value0, T1 value1, T2 value2, T3 value3, T4 value4, T5 value5, T6 value6, T7 value7, T8 value8, T9 value9) {
		return new Tuple10<>(value0,
			value1,
			value2,
			value3,
			value4,
			value5,
			value6,
			value7,
			value8,
			value9);
	}
}
