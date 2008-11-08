
package redpoll.core;

import java.util.Iterator;

public interface Vector extends Iterable<Vector.Element>{

  /**
   * Return a formatted String suitable for output
   *
   * @return
   */
  String asFormatString();
  
  /**
   * Return the cardinality of the recipient (the maximum number of values)
   *
   * @return an int
   */
  int cardinality();
  
  /**
   * Size of the vector
   * 
   * @return size of the vector
   */
  public int size();

  /**
   * Return an empty matrix of the same underlying class as the receiver
   *
   * @return a Vector
   */
  Vector like();

  /**
   * Return an empty matrix of the same underlying class as the receiver and of
   * the given cardinality
   *
   * @param cardinality
   *            an int specifying the desired cardinality
   * @return a Vector
   */
  Vector like(int cardinality);
  
  /**
   * Return a copy of the recipient
   *
   * @return a new Vector
   */
  Vector copy();
  
  /**
   * Gets the value of index
   * 
   * @param index
   * @return v(index)
   */
  public double get(int index);

  /**
   * Sets the value of index
   * 
   * @param index
   * @param value
   */
  public void set(int index, double value);
  
  /**
   * Return the value at the given index, without checking bounds
   * 
   * @param index an int index
   * @return the double at the index
   */
  public double getQuick(int index);
  
  /**
   * Set the value at the given index, without checking bounds
   * 
   * @param index an int index into the receiver
   * @param value a double value to set
   */
  public void setQuick(int index, double value);

  /**
   * Adds the value to v(index)
   * 
   * @param index
   * @param value
   */
  public Vector add(double value);

  /**
   * x = v + x
   * 
   * @param v
   * @return x = v + x
   */
  public Vector add(Vector v);

  /**
   * x dot v
   * 
   * @param v
   * @return x dot v
   */
  public double dot(Vector v);

  /**
   * v = alpha*v 
   * 
   * @param alpha
   * @return v = alpha*v
   */
  public Vector scale(double alpha);
  
 
  public static interface Element {
    /**
     * @return the value of this vector element.
     */
    double getValue();

    /**
     * @return the index of this vector element.
     */
    int getIndex();

    /**
     * @param value Set the current element to value.
     */
    void setValue(double value);
  }

  /**
   * Returns an iterator
   * 
   * @return iterator
   */
  public Iterator<Vector.Element> iterator();
  
}