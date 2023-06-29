import java.io.*;
import java.util.*;

@SuppressWarnings("ALL")
public class Page implements Serializable {
    private Vector<Hashtable<String, Object>> rows;


    public Page() {
        String filepath = "./src/main/resources/DBApp.config";
        Properties pros = new Properties();
        FileInputStream ip;
        try {
            ip = new FileInputStream(filepath);
            try {
                pros.load(ip);
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        int MaximumRowsCountinPage = Integer.parseInt(pros.getProperty("MaximumRowsCountinPage"));
        rows = new Vector<Hashtable<String, Object>>(MaximumRowsCountinPage);

    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, DBAppException {
        Page p1 = new Page();
        Hashtable<String, Object> h0 = new Hashtable<String, Object>();
        h0.put("id", 1);
        h0.put("age", 50);

        Hashtable<String, Object> h1 = new Hashtable<String, Object>();
        h1.put("id", 5);
        h1.put("age", 50);
        h1.put("name", "Ahmed");

        Hashtable<String, Object> h2 = new Hashtable<String, Object>();
        h2.put("id", 2);
        h2.put("name", "small");
        //System.out.println(p1.insert("id",h));

        Hashtable<String, Object> h3 = new Hashtable<String, Object>();
        h3.put("id", 3);
        h3.put("name", "small");

        Hashtable<String, Object> h4 = new Hashtable<String, Object>();
        h4.put("id", 4);
        h4.put("name", "small");


        System.out.println(p1.insert("id", h0) + "  " + (p1.rows.size()));
        System.out.println(p1.insert("id", h1) + "  " + (p1.rows.size()));
        System.out.println(p1.insert("id", h2) + "  " + (p1.rows.size()));
        System.out.println(p1.insert("id", h3) + "  " + (p1.rows.size()));
        System.out.println(p1.insert("id", h4));
        System.out.println(p1.rows);
    }

    public static int compareHashModified(String Pk, Hashtable<String, Object> h1, Hashtable<String, Object> h2) {

        if (h2.get(Pk) instanceof Integer) {
            if ((h1.get(Pk).equals(h2.get(Pk)))) {
                return 0;
            } else if ((int) h1.get(Pk) > (int) h2.get(Pk))
                return 1;
            else
                return -1;

        } else {
            if (h2.get(Pk) instanceof Double) {


                if ((h1.get(Pk).equals(h2.get(Pk)))) {
                    return 0;
                } else if ((double) h1.get(Pk) > (double) h2.get(Pk))
                    return 1;
                else
                    return -1;

            } else {
                if (h2.get(Pk) instanceof String) {
                    if (((String) "" + h1.get(Pk)).compareTo((String) h2.get(Pk)) == 0)
                        return 0;
                    else if (((String) "" + h1.get(Pk)).compareTo((String) h2.get(Pk)) > 0) {
                        return 1;
                    } else {
                        return -1;
                    }
                } else {
                    if (h1.get(Pk).equals(h2.get(Pk)))
                        return 0;
                    else if (((Date) h1.get(Pk)).compareTo((Date) h2.get(Pk)) > 0)
                        return 1;
                    else
                        return -1;
                }
            }
        }
    }

    public Hashtable<String, Object> insert(String Pk, Hashtable<String, Object> input) throws DBAppException {
        if (rows.isEmpty()) {
            rows.add(input);
            return null;
        } else if (compareHash(Pk, rows.get(0), input) > 0) {
            return shift(0, input);
        } else if (compareHash(Pk, rows.get((rows.size() - 1)), input) < 0) {
            if (!isFull()) {
                rows.add(input);
                return null;
            }
            return input;
        }
        int l = 0, r = rows.size() - 1;
        while (l <= r) {
            int m = l + (r - l) / 2;

            // Check if x is present at mid
            if (compareHash(Pk, rows.get(m), input) == 0) {
                throw new DBAppException("A record with the same Primary Key is in the Table");
            }

            // If x greater, ignore left half
            else if (compareHash(Pk, rows.get(m), input) < 0) {
                if (compareHash(Pk, rows.get(m + 1), input) > 0)
                    return shift(m + 1, input);
                l = m + 1;
            }

            // If x is smaller, ignore right half
            else if (compareHash(Pk, rows.get(m), input) > 0) {
                if (compareHash(Pk, rows.get(m - 1), input) < 0)
                    return shift(m, input);
                r = m - 1;
            }

        }
        return null;
    }

    public Hashtable<String, Object> shift(int x, Hashtable<String, Object> input) {
        Hashtable<String, Object> h = null;
        if (isFull())
            h = rows.remove(rows.size() - 1);
        rows.insertElementAt(input, x);
        return h;
    }

    public void delete(Hashtable<String, Object> deleted, Vector<String> indexes) {
        for (int i = 0; i < rows.size(); i++) {
            boolean flag = true;
            Enumeration<String> asw = deleted.keys();
            while (asw.hasMoreElements()) {
                String ColumnName = asw.nextElement();
                Object value = deleted.get(ColumnName);
                Object comp = rows.get(i).get(ColumnName);
                if (!value.equals(comp))
                    flag = false;
            }
            if (flag) {
                updateIndexes(rows.get(i), null, indexes, null);
                rows.remove(i);
                i--;
            }
        }

    }

    public void parsePK(String key, String value, Hashtable<String, Object> toUpdate) {
        if (rows.get(0).get(key) instanceof Integer) {
            int values = Integer.parseInt(value);
            toUpdate.put(key, values);
        } else if (rows.get(0).get(key) instanceof Double) {
            double values = Double.parseDouble(value);
            toUpdate.put(key, values);
        } else if (rows.get(0).get(key) instanceof String)
            toUpdate.put(key, value);
        else if (rows.get(0).get(key) instanceof Date) {
            int year = Integer.parseInt(value.trim().substring(0, 4));
            int month = Integer.parseInt(value.trim().substring(5, 7));
            int day = Integer.parseInt(value.trim().substring(8));

            Date dob = new Date(year, month, day);
            toUpdate.put(key, dob);
        }
    }

    public boolean update(String Pk, String searchingFor, Hashtable<String, Object> toUpdate, Vector<String> indexes, String filepath) {
        parsePK(Pk, searchingFor, toUpdate);
        int l = 0, r = rows.size() - 1;
        while (l <= r) {
            int m = l + (r - l) / 2;

            // Check if x is present at mid
            if (compareHash(Pk, rows.get(m), toUpdate) == 0) {
                Hashtable<String, Object> old = rows.get(m);
                updateRow(rows.get(m), toUpdate);
                updateIndexes(old, rows.get(m), indexes, filepath);
                return true;
            }

            // If x greater, ignore left half
            if (compareHash(Pk, rows.get(m), toUpdate) < 0)
                l = m + 1;

                // If x is smaller, ignore right half
            else
                r = m - 1;
        }

        // if we reach here, then element was
        // not present
        return false;
    }

    private void updateIndexes(Hashtable<String, Object> old, Hashtable<String, Object> newest, Vector<String> indexes, String filepath) {
        for (int i = 0; i < indexes.size(); i++) {
            Index ind = deserializeIndex(indexes.get(i));
            ind.replace(newest, old, filepath);
            serializeIndex(indexes.get(i), ind);
        }

    }

    public void updateRow(Hashtable<String, Object> h1, Hashtable<String, Object> h2) {
        Enumeration<String> asw = h2.keys();
        while (asw.hasMoreElements()) {
            String ColumnName = asw.nextElement();
            Object value = h2.get(ColumnName);
            if (h1.containsKey(ColumnName))
                h1.replace(ColumnName, value);
            else
                h1.put(ColumnName, value);
        }
    }

    public boolean isFull() {
        return rows.capacity() == rows.size();
    }

    public int compareHash(String Pk, Hashtable<String, Object> h1, Hashtable<String, Object> h2) {
        if (h2.get(Pk) instanceof Integer) {
            if ((h1.get(Pk).equals(h2.get(Pk)))) {
                return 0;
            } else if ((int) h1.get(Pk) > (int) h2.get(Pk))
                return 1;
            else
                return -1;
        } else if (h2.get(Pk) instanceof Double) {
            if ((double) h1.get(Pk) == (double) h2.get(Pk))
                return 0;
            else if ((double) h1.get(Pk) > (double) h2.get(Pk))
                return 1;
            else
                return -1;
        } else if (h2.get(Pk) instanceof String) {
            if (((String) "" + h1.get(Pk)).compareTo((String) h2.get(Pk)) == 0)
                return 0;
            else if (((String) "" + h1.get(Pk)).compareTo((String) h2.get(Pk)) > 0) {
                return 1;
            } else {
                return -1;
            }
        } else {
            if (h1.get(Pk).equals(h2.get(Pk)))
                return 0;
            else if (((Date) h1.get(Pk)).compareTo((Date) h2.get(Pk)) > 0)
                return 1;
            else
                return -1;
        }
    }

    public Vector<Hashtable<String, Object>> getRows() {
        return rows;
    }

    public void serializeIndex(String s, Index ind) {


        try {
            FileOutputStream fileOut = new FileOutputStream(s);
            ObjectOutputStream out = new ObjectOutputStream(fileOut);
            out.writeObject(ind);
            out.close();
            fileOut.close();
        } catch (IOException i) {
            i.printStackTrace();
        }
    }

    public Index deserializeIndex(String s) {

        FileInputStream fileIn;
        try {
            fileIn = new FileInputStream(s);
            ObjectInputStream in = new ObjectInputStream(fileIn);
            Index ind = (Index) in.readObject();
            in.close();
            fileIn.close();
            return ind;
        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return null;
    }

}
