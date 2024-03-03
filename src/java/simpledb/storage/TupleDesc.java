package simpledb.storage;

import simpledb.common.Type;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {
    /**
     * List of fields as an attribute of TupleDesc
     */
    private ArrayList<TDItem> fieldsList;

    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;
        
        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        // some code goes here
        return this.fieldsList.iterator();
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        // some code goes here
        if (typeAr.length == 0 | fieldAr.length == 0) {
            throw new IllegalArgumentException("Empty array(s)!");
        }

        if (typeAr.length != fieldAr.length) {
            throw new IllegalArgumentException("The length of the 2 arrays are not the same!");
        }

        ArrayList<TDItem> temp = new ArrayList<>();

        for (int i = 0; i < typeAr.length; i++){
            TDItem cur_field = new TDItem(typeAr[i], fieldAr[i]);
            temp.add(cur_field);
        }

        this.fieldsList = temp;
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        // some code goes here
        if (typeAr.length == 0) {
            throw new IllegalArgumentException("Empty array!");
        }

        ArrayList<TDItem> temp = new ArrayList<>();
        for (int i = 0; i < typeAr.length; i++){
            TDItem cur_field = new TDItem(typeAr[i], "");
            temp.add(cur_field);
        }
        this.fieldsList = temp;
    }
    /**
     * Constructor. Create a new typle desc with ArrayList of TDItem
     * @param tditems
     */

    public TupleDesc(ArrayList<TDItem> tditems) {
        if (tditems == null || tditems.isEmpty()) {
            throw new IllegalArgumentException("The ArrayList of TDItem is empty");
        }

        this.fieldsList = new ArrayList<>(tditems);
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        int size = this.fieldsList.size();
        return size;
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here
        if (i < 0 || i > this.numFields() - 1) {
            throw new NoSuchElementException("Invalid field reference.");
        }
        String name = this.fieldsList.get(i).fieldName;
        return name;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        // some code goes here
        if (i < 0 || i > this.numFields() - 1) {
            throw new NoSuchElementException("Invalid field reference.");
        }
        Type t = this.fieldsList.get(i).fieldType;
        return t;
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        // some code goes here
        for (int i = 0; i < this.numFields(); i ++){
            TDItem curTDItem = this.fieldsList.get(i);
            if (curTDItem.fieldName.equals(name)){
                return i;
            }
        }
       throw new NoSuchElementException("No field with such name found");
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
        int size = 0;

        for (int i = 0; i < this.numFields(); i ++){
            size += this.getFieldType(i).getLen();
        }

        return size;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        // some code goes here
        String[] sAr = new String[td1.numFields() + td2.numFields()];
        Type[] tAr = new Type[td1.numFields() + td2.numFields()];
        
        for (int i = 0; i < td1.numFields(); i ++){
            sAr[i] = td1.getFieldName(i);
            tAr[i] = td1.getFieldType(i);
        }

        for (int i = td1.numFields(); i < td1.numFields() + td2.numFields(); i ++){
            sAr[i] = td2.getFieldName(i);
            tAr[i] = td2.getFieldType(i);
        }

        return new TupleDesc(tAr, sAr);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */

    public boolean equals(Object o) {
        // some code goes here
        if (!(o instanceof TupleDesc)) {
            return false;
        }

        // Cast as a TupleDesc object to access specific class methods
        TupleDesc td = (TupleDesc) o;

        // Check size
        if (this.numFields() != td.numFields()) {
            return false;
        }

        // Check all n field types
        for (int i = 0; i < this.numFields(); i++) {
            if (!this.getFieldType(i).equals(td.getFieldType(i))) {
                return false;
            }
        }
        return true;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        // some code goes here
        String result = "";
        for (int i = 0; i < this.numFields(); i++){
            if (i < this.numFields() - 1){
                result = result + this.fieldsList.get(i).toString() + ",";
            }
            else {
                result = result + this.fieldsList.get(i).toString();
            }
        }
        return result;
    }
}
