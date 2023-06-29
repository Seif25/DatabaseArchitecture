import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Hashtable;
import java.util.Properties;
import java.util.Vector;

@SuppressWarnings("ALL")
public class Bucket implements Serializable {
    Vector<String> bucket;

    public Bucket() {
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

        int MaximumKeysCountinIndexBucket = Integer.parseInt(pros.getProperty("MaximumKeysCountinIndexBucket"));
        bucket = new Vector<String>(MaximumKeysCountinIndexBucket);
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
                    if (((Date) h1.get(Pk)).equals((Date) h2.get(Pk)))
                        return 0;
                    else if (((Date) h1.get(Pk)).compareTo((Date) h2.get(Pk)) > 0)
                        return 1;
                    else
                        return -1;
                }
            }
        }
    }

    public static Vector<String> intersection(Vector<String> list1, Vector<String> list2) {
        Vector<String> result = new Vector<String>();

        for (int i = 0; i < list1.size(); i++) {
            if (list2.contains(list1.get(i))) {
                result.add(list1.get(i));
            }
        }
        // System.out.println(result.size());
        return result;
    }

    public static Vector<String> xor(Vector<String> list1, Vector<String> list2) {
        Vector<String> result = new Vector<String>();
        result.addAll(list1);
        for (int i = 0; i < list2.size(); i++) {
            if (list1.contains(list2.get(i))) {
                result.remove(list2.get(i));
            } else {
                result.add(list2.get(i));
            }
        }
        // System.out.println(result.size());
        return result;
    }

    public static Vector<String> union(Vector<String> list1, Vector<String> list2) {
        Vector<String> result = new Vector<String>();
        result.addAll(list1);

        for (int i = 0; i < list2.size(); i++) {
            if (!result.contains(list2.get(i))) {
                result.add(list2.get(i));
            }
        }

        return result;
    }

    public Vector<String> getBucket() {
        return bucket;
    }

    public boolean isFull() {
        return bucket.size() >= bucket.capacity();
    }

    public void insert(Hashtable<String, Object> input, String filepath, String[] list, String PK) {
        if (!isFull()) {
            String s = filepath + "," + input.get(PK);
            for (int i = 0; i < list.length; i++) {
                s += "," + input.get(list[i]);
            }
            int l = 0, r = bucket.size() - 1;
            while (l <= r) {
                int m = l + (r - l) / 2;
                String[] primary = bucket.get(m).split(",");
                // Check if x is present at mid
                if (compares(input.get(PK), primary[1]) == 0) {
                    bucket.remove(m);
                    bucket.insertElementAt(s, m);
                    return;
                }

                // If x greater, ignore left half
                if (compares(input.get(PK), primary[1]) > 0) {
                    if (m == bucket.size() - 1) {
                        bucket.add(s);
                        return;
                    } else {
                        String[] primary2 = bucket.get(m + 1).split(",");
                        if (compares(input.get(PK), primary2[1]) < 0) {
                            bucket.insertElementAt(s, m + 1);
                            return;
                        }
                    }
                    l = m + 1;
                }
                // If x is smaller, ignore right half
                else {
                    if (m == 0) {
                        bucket.insertElementAt(s, 0);
                        return;
                    } else {
                        String[] primary2 = bucket.get(m - 1).split(",");
                        if (compares(input.get(PK), primary2[1]) > 0) {
                            bucket.insertElementAt(s, m);
                            return;
                        }
                    }
                    r = m - 1;
                }
            }
        }

        //else overflows(not implemented)
    }

    public Vector<String> getpagesdeleted(Hashtable<String, Object> deleted, String[] list, String Pk) {
        if (deleted.get(Pk) != null) {
            Vector<String> p = new Vector<String>();
            p.add(getPage(deleted.get(Pk), 1));
            return p;
        } else {
            Vector<String> pages = new Vector<String>();
            for (int i = 0; i < bucket.size(); i++) {
                String[] b1 = bucket.get(i).split(",");
                boolean flag = true;
                for (int j = 2; j < b1.length; j++) {
                    if (!b1[j].equals("null")) {
                        if (deleted.get(list[j - 2]) != null && compares(deleted.get(list[j - 2]), b1[j]) != 0) {
                            flag = false;
                        }
                    }
                }
                if (flag) {
                    boolean flag2 = true;
                    for (int j = 0; j < pages.size(); j++) {
                        if (pages.get(j).equals(b1[0]))
                            flag2 = false;
                    }
                    if (flag2)
                        pages.add(b1[0]);
                }
            }
            return pages;
        }
    }

    private int compares(Object object, String string) {
        if (object instanceof Integer) {
            if ((object.equals(Integer.parseInt(string)))) {
                return 0;
            } else if ((int) object > Integer.parseInt(string))
                return 1;
            else
                return -1;
        } else if (object instanceof Double) {
            if ((double) object == Double.parseDouble(string))
                return 0;
            else if ((double) object > Double.parseDouble(string))
                return 1;
            else
                return -1;
        } else if (object instanceof Date) {
            DateFormat format = new SimpleDateFormat("EEEEE MMMM dd HH:mm:ss zzzz yyyy");
            Date date1;
            try {
                date1 = format.parse(string);
                if (object.equals(string))
                    return 0;
                else if (((Date) object).compareTo(date1) > 0)
                    return 1;
                else
                    return -1;
            } catch (ParseException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }

        }


        if (((String) "" + object).compareTo(string) == 0)
            return 0;
        else if (((String) "" + object).compareTo((string)) > 0) {
            return 1;
        } else {
            return -1;
        }


    }

    public String getPage(Object input, int key) {
        int l = 0, r = bucket.size() - 1;
        while (l <= r) {
            int m = l + (r - l) / 2;
            String[] primary = bucket.get(m).split(",");
            // Check if x is present at mid
            if (compares(input, primary[1]) == 0) {
                return primary[0];
            }

            // If x greater, ignore left half
            if (compares(input, primary[1]) > 0) {
                if (key == 0) {
                    if (m == bucket.size() - 1) {
                        return primary[0];
                    } else {
                        String[] primary2 = bucket.get(m + 1).split(",");
                        if (compares(input, primary2[1]) < 0) {
                            return primary[0];
                        }
                    }
                }
                l = m + 1;
            }
            // If x is smaller, ignore right half
            else {
                if (key == 0) {
                    if (m == 0) {
                        return null;
                    } else {
                        String[] primary2 = bucket.get(m - 1).split(",");
                        if (compares(input, primary2[1]) > 0) {
                            return primary2[0];
                        }
                    }
                }
                r = m - 1;
            }
        }
        return null;
    }

    public void deleteFromBucket(Hashtable<String, Object> deleted, String[] list, String pK) {
        if (deleted.get(pK) != null) {
            int l = 0, r = bucket.size() - 1;
            while (l <= r) {
                int m = l + (r - l) / 2;
                String[] primary = bucket.get(m).split(",");
                // Check if x is present at mid
                if (compares(deleted.get(pK), primary[1]) == 0) {
                    bucket.remove(m);
                    return;
                }

                // If x greater, ignore left half
                if (compares(deleted.get(pK), primary[1]) > 0) {
                    l = m + 1;
                }
                // If x is smaller, ignore right half
                else {
                    r = m - 1;
                }
            }
        }

    }

    public Vector<String> getSelectPages(SQLTerm[] sqlTerms, String[] arrayOperators, String[] list, String[] listtypes) {
        Vector<String> result = new Vector<String>();
        Vector<String> semiresult = new Vector<String>();

        boolean firstquery = true;
        for (int z = 0; z < sqlTerms.length; z++) {


            Hashtable<String, Object> h1 = new Hashtable<String, Object>();
            h1.put(sqlTerms[z]._strColumnName, sqlTerms[z]._objValue);


            for (int i = 0; i < bucket.size(); i++) {
                Hashtable<String, Object> h2 = new Hashtable<String, Object>();
                String[] s = bucket.get(i).split(",");
                String ColumnName = sqlTerms[z]._strColumnName;
                for (int j = 2; j < s.length; j++) {
                    if (!(s[j].equals("null"))) {
                        if (sqlTerms[z]._strColumnName.equals(list[j - 2])) {

                            switch (listtypes[j - 2]) {
                                case ("java.lang.Integer"):
                                    h2.put(sqlTerms[z]._strColumnName, Integer.parseInt(s[j]));
                                    break;
                                case ("java.lang.String"):
                                    h2.put(list[j - 2], s[j]);
                                    break;
                                case ("java.lang.Double"):
                                    h2.put(list[j - 2], Double.parseDouble(s[j]));
                                    break;
                                case ("java.util.Date"):
                                    DateFormat format = new SimpleDateFormat("EEEEE MMMM dd HH:mm:ss zzzz yyyy");
                                    Date date1;
                                    try {
                                        date1 = format.parse(s[j]);
                                        h2.put(list[j - 2], date1);
                                    } catch (ParseException e) {
                                        // TODO Auto-generated catch block
                                        e.printStackTrace();
                                    }
                                    break;
                            }
                            switch (sqlTerms[z]._strOperator) {
                                case (">"):
                                    if (compareHashModified(ColumnName, h2, h1) == 1) semiresult.add(s[0] + "," + s[1]);
                                    break;
                                case (">="):
                                    if (compareHashModified(ColumnName, h2, h1) == 1 ||
                                            compareHashModified(ColumnName, h2, h1) == 0)
                                        semiresult.add(s[0] + "," + s[1]);
                                    break;
                                case ("<"):
                                    if (compareHashModified(ColumnName, h2, h1) == -1)
                                        semiresult.add(s[0] + "," + s[1]);
                                    break;
                                case ("<="):
                                    if (compareHashModified(ColumnName, h2, h1) == -1 ||
                                            compareHashModified(ColumnName, h2, h1) == 0)
                                        semiresult.add(s[0] + "," + s[1]);
                                    break;

                                case ("="):
                                    if (compareHashModified(ColumnName, h2, h1) == 0) semiresult.add(s[0] + "," + s[1]);
                                    break;
                                case ("!="):
                                    if (compareHashModified(ColumnName, h2, h1) != 0) semiresult.add(s[0] + "," + s[1]);
                                    break;
                            }
                        }

                    }

                    ////Checking with 3 Queries
                }

            }
            if (firstquery) {

                result.addAll((Vector<String>) semiresult); //result=semiresult
            } else {
                switch (arrayOperators[z - 1]) {
                    case "AND":
                        result = intersection(result, semiresult);
                        break;
                    case "OR":
                        result = union(result, semiresult);
                        break;
                    case "XOR":
                        result = xor(result, semiresult);
                        break;
                    ///////XOR NOT Handled YET

                }
            }
            firstquery = false;
            semiresult.removeAllElements();
        }

        Vector<String> finalresult = new Vector<String>();
        for (int i = 0; i < result.size(); i++) {
            String[] s = result.get(i).split(",");
            finalresult.add(s[0]);
        }
        return finalresult;
    }


}