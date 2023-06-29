import java.io.*;
import java.text.ParseException;
import java.util.*;

@SuppressWarnings("ALL")
public class Table implements Serializable {
    private static final long serialVersionUID = -1745117432708698462L;
    private String Name;
    private String Pk;
    private Vector<String> pages;
    private Vector<String> indexes;
    private int pagecounter;
    private int indexcounter;

    public Table(String Name, String Pk) {
        this.Name = Name;
        this.Pk = Pk;
        pages = new Vector<String>();
        indexes = new Vector<String>();
        createIndex(new String[]{Pk});
    }

    public static void checker(String tablename, Hashtable<String, Object> enteredvalues) throws DBAppException, ParseException {
        String line = "";
        String splitBy = ",";
        try {
            BufferedReader br = new BufferedReader(new FileReader(
                    "src\\main\\resources\\metadata.csv"));
            br.readLine();
            while ((line = br.readLine()) != null) // returns a Boolean value
            {
                String[] data = line.split(splitBy); // use comma as separator
                if (data[0].equals(tablename)) {
                    if (!(enteredvalues.containsKey(data[1]))) {
                        if (data[3].equals("True"))
                            throw new DBAppException("Not Enough Data is Entered by user (Not Primary Key)");

                    } else {
                        if (data[2].equals("java.lang.Integer")) {
                            if (!(enteredvalues.get(data[1]) instanceof java.lang.Integer)) {
                                throw new DBAppException("Data Should be Integer");
                            } else {
                                if (!((int) enteredvalues.get(data[1]) >= Integer.parseInt(data[5]) &&
                                        (int) enteredvalues.get(data[1]) <= Integer.parseInt(data[6]))) {
                                    throw new DBAppException("One or more of the entered integers data is not In Range of maximum and minimum ");

                                }
                            }

                        } else {
                            if (data[2].equals("java.lang.String")) {
                                if (!(enteredvalues.get(data[1]) instanceof java.lang.String)) {
                                    throw new DBAppException("Data Should be String");
                                } else {
                                    if (!(((String) enteredvalues.get(data[1])).compareTo(data[5]) > 0 &&
                                            (((String) enteredvalues.get(data[1])).compareTo(data[6]) < 0))) {
                                        throw new DBAppException(
                                                "One or more of the entered String data is not In Range of maximum and minimum ");
                                    }
                                }


                            } else {
                                if (data[2].equals("java.lang.Double")) {
                                    if (!(enteredvalues.get(data[1]) instanceof java.lang.Double)) {
                                        throw new DBAppException("Data Should be Double");
                                    } else {
                                        if (!(((double) enteredvalues.get(data[1])) >= Double.parseDouble(data[5]) &&
                                                (((double) enteredvalues.get(data[1])) <= Double.parseDouble(data[6])))) {
                                            throw new DBAppException("One or more of the entered Double data is not In Range of maximum and minimum ");

                                        }

                                    }


                                } else {
                                    if (data[2].equals("java.util.Date")) {
                                        if (!(enteredvalues.get(data[1]) instanceof java.util.Date)) {
                                            throw new DBAppException("Data Should be Date");
                                        } else {
                                            int year = Integer.parseInt(data[5].trim().substring(0, 4));
                                            int month = Integer.parseInt(data[5].trim().substring(5, 7));
                                            int day = Integer.parseInt(data[5].trim().substring(8));

                                            Date date1 = new Date(year - 1900, month - 1, day);
                                            int year1 = Integer.parseInt(data[6].trim().substring(0, 4));
                                            int month1 = Integer.parseInt(data[6].trim().substring(5, 7));
                                            int day1 = Integer.parseInt(data[6].trim().substring(8));

                                            Date date2 = new Date(year1 - 1900, month1 - 1, day1);
                                            //Date date1=new SimpleDateFormat("yyyy-mm-dd").parse(data[5]); //small
                                            //Date date2=new SimpleDateFormat("yyyy-mm-dd").parse(data[6]);
                                            // System.out.println(date1);
                                            // System.out.println(date2); //large
                                            // System.out.println((enteredvalues.get("mine")));
                                            //System.out.println(data[5]);
                                            //System.out.println(data[6]);

                                            if (((Date) enteredvalues.get(data[1])).after(date1) &&
                                                    (((Date) enteredvalues.get(data[1])).before(date2))) ;
                                            else

                                                throw new DBAppException("One or more of the Dates entered is not In Range of maximum and minimum ");


                                        }
                                    } else {
                                        System.out.println("Data Not Supported");
                                    }
                                }
                            }
                        }
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void checkerIn(String tablename, Hashtable<String, Object> enteredvalues) throws DBAppException, ParseException {
        String line = "";
        String splitBy = ",";
        try {
            BufferedReader br = new BufferedReader(new FileReader(
                    "src\\main\\resources\\metadata.csv"));
            br.readLine();

            while ((line = br.readLine()) != null) // returns a Boolean value
            {
                String[] data = line.split(splitBy); // use comma as separator
                if (data[0].equals(tablename) && enteredvalues.containsKey(data[1])) {

                    if (data[2].equals("java.lang.Integer")) {
                        if (!(enteredvalues.get(data[1]) instanceof java.lang.Integer)) {
                            throw new DBAppException("Data Should be Integer");
                        } else {
                            if (!((int) enteredvalues.get(data[1]) >= Integer.parseInt(data[5]) &&
                                    (int) enteredvalues.get(data[1]) <= Integer.parseInt(data[6]))) {
                                throw new DBAppException("One or more of the entered integers data is not In Range of maximum and minimum ");

                            }
                        }

                    } else {
                        if (data[2].equals("java.lang.String")) {
                            if (!(enteredvalues.get(data[1]) instanceof java.lang.String)) {
                                throw new DBAppException("Data Should be String");
                            } else {
                                if (!(((String) enteredvalues.get(data[1])).compareTo(data[5]) > 0 &&
                                        (((String) enteredvalues.get(data[1])).compareTo(data[6]) < 0))) {
                                    throw new DBAppException(
                                            "One or more of the entered String data is not In Range of maximum and minimum ");
                                }
                            }


                        } else {
                            if (data[2].equals("java.lang.Double")) {
                                if (!(enteredvalues.get(data[1]) instanceof java.lang.Double)) {
                                    throw new DBAppException("Data Should be Double");
                                } else {
                                    if (!(((double) enteredvalues.get(data[1])) >= Double.parseDouble(data[5]) &&
                                            (((double) enteredvalues.get(data[1])) <= Double.parseDouble(data[6])))) {
                                        throw new DBAppException("One or more of the entered Double data is not In Range of maximum and minimum ");

                                    }

                                }


                            } else {
                                if (data[2].equals("java.util.Date")) {
                                    if (!(enteredvalues.get(data[1]) instanceof java.util.Date)) {
                                        throw new DBAppException("Data Should be Date");
                                    } else {
                                        int year = Integer.parseInt(data[5].trim().substring(0, 4));
                                        int month = Integer.parseInt(data[5].trim().substring(5, 7));
                                        int day = Integer.parseInt(data[5].trim().substring(8));

                                        Date date1 = new Date(year - 1900, month - 1, day);
                                        int year1 = Integer.parseInt(data[6].trim().substring(0, 4));
                                        int month1 = Integer.parseInt(data[6].trim().substring(5, 7));
                                        int day1 = Integer.parseInt(data[6].trim().substring(8));

                                        Date date2 = new Date(year1 - 1900, month1 - 1, day1);
                                        // Date date1=new SimpleDateFormat("yyyy-mm-dd").parse(data[5]); //small
                                        //Date date2=new SimpleDateFormat("yyyy-mm-dd").parse(data[6]);
                                        //  System.out.println(date1);
                                        // System.out.println(date2); //large
                                        // System.out.println((enteredvalues.get("mine")));
                                        // System.out.println(((Date)enteredvalues.get(data[1])).before(date2));
                                        //System.out.println(((Date)enteredvalues.get(data[1])).after(date1));

                                        if (((Date) enteredvalues.get(data[1])).compareTo((date1)) > 0 &&
                                                (((Date) enteredvalues.get(data[1])).compareTo((date2))) < 0) ;
                                        else

                                            throw new DBAppException("One or more of the Dates entered is not In Range of maximum and minimum ");


                                    }
                                } else {
                                    throw new DBAppException("Data Not Supported");
                                }
                            }
                        }
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void checkcolumnexists(String tablename, Hashtable<String, Object> enteredvalues) throws DBAppException {
        String line = "";
        String splitBy = ",";
        try {
            Enumeration<String> asw = enteredvalues.keys();
            while (asw.hasMoreElements()) {
                String ColumnName = asw.nextElement();
                boolean flag = false;

                BufferedReader br = new BufferedReader(new FileReader(
                        "src\\main\\resources\\metadata.csv"));
                br.readLine();
                while ((line = br.readLine()) != null) // returns a Boolean value
                {
                    String[] data = line.split(splitBy); // use comma as separator
                    if (data[0].equals(tablename)) {
                        if (ColumnName.equals(data[1])) {
                            flag = true;
                        }

                    }

                }
                if (flag == false)
                    throw new DBAppException("The Column you are trying to insert in doesnt exist");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static ArrayList intersection(ArrayList list1, ArrayList list2) {
        ArrayList result = new ArrayList();

        for (int i = 0; i < list1.size(); i++) {
            if (list2.contains(list1.get(i))) {
                result.add(list1.get(i));
            }
        }
        // System.out.println(result.size());
        return result;
    }

    public static ArrayList xor(ArrayList list1, ArrayList list2) {
        ArrayList result = new ArrayList();
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

    public static ArrayList union(ArrayList list1, ArrayList list2) {
        ArrayList result = new ArrayList();
        result.addAll(list1);

        for (int i = 0; i < list2.size(); i++) {
            if (!result.contains(list2.get(i))) {
                result.add(list2.get(i));
            }
        }

        return result;
    }

    public static ArrayList linearsearch(SQLTerm[] sqlTerms, String[] arrayOperators, Page p) {
        //Linear Search in all pages
        ArrayList result = new ArrayList();
        ArrayList semiresult = new ArrayList();

        boolean firstquery = true;
        for (int z = 0; z < sqlTerms.length; z++) {


            Hashtable<String, Object> h1 = new Hashtable<String, Object>();
            h1.put(sqlTerms[z]._strColumnName, sqlTerms[z]._objValue);


            for (int j = 0; j < p.getRows().size(); j++) {
                if (p.getRows().get(j).containsKey(sqlTerms[z]._strColumnName)) {
                    Hashtable<String, Object> h2 = new Hashtable<String, Object>();
                    Object value = p.getRows().get(j).get(sqlTerms[z]._strColumnName);
					/*	String ColumnName="";
						Object value=null;
						Enumeration<String> asw = p.getRows().get(j).keys();
						boolean flagfound=false;
						while (asw.hasMoreElements()&& !flagfound) {
							ColumnName=asw.nextElement();
							if(ColumnName.equals(sqlTerms[z]._strColumnName))
							{
								 value = p.getRows().get(j).get(ColumnName);
								 flagfound=true;
							}
						}
						////

						*
						*/
                    String ColumnName = sqlTerms[z]._strColumnName;
                    if (sqlTerms[z]._objValue instanceof Double) {
                        double d = (Double) value;
                        h2.put(ColumnName, d);
                    } else {
                        if (sqlTerms[z]._objValue instanceof String) {
                            String d = (String) value;
                            h2.put(ColumnName, d);
                        } else {
                            if (sqlTerms[z]._objValue instanceof Integer) {
                                int d = (int) value;
                                h2.put(ColumnName, d);
                            } else {
                                Date d = (Date) value;
                                h2.put(ColumnName, d);

                            }
                        }
                    }

                    switch (sqlTerms[z]._strOperator) {
                        case (">"):
                            if (p.compareHashModified(ColumnName, h2, h1) == 1) semiresult.add(p.getRows().get(j));
                            break;
                        case (">="):
                            if (p.compareHashModified(ColumnName, h2, h1) == 1 ||
                                    p.compareHashModified(ColumnName, h2, h1) == 0) semiresult.add(p.getRows().get(j));
                            break;
                        case ("<"):
                            if (p.compareHashModified(ColumnName, h2, h1) == -1) semiresult.add(p.getRows().get(j));
                            break;
                        case ("<="):
                            if (p.compareHashModified(ColumnName, h2, h1) == -1 ||
                                    p.compareHashModified(ColumnName, h2, h1) == 0) semiresult.add(p.getRows().get(j));
                            break;

                        case ("="):
                            if (p.compareHashModified(ColumnName, h2, h1) == 0) semiresult.add(p.getRows().get(j));
                            break;
                        case ("!="):
                            if (p.compareHashModified(ColumnName, h2, h1) != 0) semiresult.add(p.getRows().get(j));
                            break;


                    }


                }


            }
            if (firstquery) {

                result.addAll((ArrayList) semiresult); //result=semiresult
                firstquery = false;
                semiresult.removeAll(semiresult);
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
                semiresult.removeAll(semiresult);
            }

            ////Checking with 3 Queries

        }
        System.out.println(result.size());
        return result;
    }

    public static boolean compareStrings(String[] a, String[] b) {
        for (int i = 0; i < a.length; i++) {
            boolean found = false;
            for (int j = 0; j < b.length; j++) {
                if (a[i].equals(b[j])) {
                    found = true;
                    break;

                }


            }
            if (!found)
                return false;
        }
        return true;
    }

    public static void main(String[] args) throws IOException, ReflectiveOperationException, DBAppException, ParseException {

    }

    public void insert(Hashtable<String, Object> input) throws DBAppException {
        try {
            checkcolumnexists(Name, input);
            checker(Name, input);
            if (pages.isEmpty()) {
                Page p = new Page();
                p.getRows().add(input);
                pages.add("./src/main/resources/data/" + Name + pagecounter + ".ser");
                serializePage("./src/main/resources/data/" + Name + pagecounter + ".ser", p);
                for (int i = 0; i < indexes.size(); i++) {
                    Index ind = deserializeIndex(indexes.get(i));
                    ind.insertinBucket("./src/main/resources/data/" + Name + pagecounter + ".ser", input);
                    serializeIndex(indexes.get(i), ind);
                }
                pagecounter++;
                return;
            } else {
                Index ind = deserializeIndex(indexes.get(0));
                String filepath = ind.getPage(input, 0);
                if (filepath.equals("zero")) {
                    filepath = pages.get(0);
                }
                int x = getpagenum(filepath);
                serializeIndex(indexes.get(0), ind);
                shift(input, x);

            }
        } catch (ParseException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    private int getpagenum(String filepath) {
        for (int i = 0; i < pages.size(); i++) {
            if (pages.get(i).equals(filepath))
                return i;
        }
        return 0;
    }

    public void shift(Hashtable<String, Object> h, int pagenum) throws DBAppException {
        Page p = deserializePage(pages.get(pagenum));
        if (!p.isFull()) {
            p.insert(Pk, h);
            for (int i = 0; i < indexes.size(); i++) {
                Index ind = deserializeIndex(indexes.get(i));
                ind.insertinBucket(pages.get(pagenum), h);
                serializeIndex(indexes.get(i), ind);
            }
        } else {
            if (pagenum != pages.size() - 1) {
                Page p1 = deserializePage(pages.get(pagenum + 1));
                if (!p1.isFull()) {
                    Hashtable<String, Object> h1 = p.insert(Pk, h);
                    for (int i = 0; i < indexes.size(); i++) {
                        Index ind = deserializeIndex(indexes.get(i));
                        ind.insertinBucket(pages.get(pagenum), h);
                        serializeIndex(indexes.get(i), ind);
                    }
                    p1.insert(Pk, h1);
                    for (int i = 0; i < indexes.size(); i++) {
                        Index ind = deserializeIndex(indexes.get(i));
                        ind.insertinBucket(pages.get(pagenum + 1), h1);
                        serializeIndex(indexes.get(i), ind);
                    }
                    serializePage(pages.get(pagenum + 1), p1);
                } else {
                    Page p2 = new Page();
                    Hashtable<String, Object> h1 = p.insert(Pk, h);
                    for (int i = 0; i < indexes.size(); i++) {
                        Index ind = deserializeIndex(indexes.get(i));
                        ind.insertinBucket(pages.get(pagenum), h);
                        serializeIndex(indexes.get(i), ind);
                    }
                    p2.insert(Pk, h1);
                    serializePage("./src/main/resources/data/" + Name + pagecounter + ".ser", p2);
                    pages.insertElementAt("./src/main/resources/data/" + Name + pagecounter + ".ser", pagenum + 1);
                    pagecounter++;
                    for (int i = 0; i < indexes.size(); i++) {
                        Index ind = deserializeIndex(indexes.get(i));
                        ind.insertinBucket(pages.get(pagenum + 1), h1);
                        serializeIndex(indexes.get(i), ind);
                    }

                }
            } else {
                Page p2 = new Page();
                Hashtable<String, Object> h1 = p.insert(Pk, h);
                for (int i = 0; i < indexes.size(); i++) {
                    Index ind = deserializeIndex(indexes.get(i));
                    ind.insertinBucket(pages.get(pagenum), h);
                    serializeIndex(indexes.get(i), ind);
                }
                p2.insert(Pk, h1);
                serializePage("./src/main/resources/data/" + Name + pagecounter + ".ser", p2);
                pages.insertElementAt("./src/main/resources/data/" + Name + pagecounter + ".ser", pagenum + 1);
                pagecounter++;
                for (int i = 0; i < indexes.size(); i++) {
                    Index ind = deserializeIndex(indexes.get(i));
                    ind.insertinBucket(pages.get(pagenum + 1), h1);
                    serializeIndex(indexes.get(i), ind);
                }

            }
        }
        serializePage(pages.get(pagenum), p);
    }

    public void delete(Hashtable<String, Object> deleted) throws DBAppException {
        checkcolumnexists(Name, deleted);
        if (deleted.containsKey(Pk)) {
            Index ind = deserializeIndex(indexes.get(0));
            String s = ind.getPage(deleted, 1);
            serializeIndex(indexes.get(0), ind);
            if (s.equals("zero"))
                return;
            Page p = deserializePage(s);
            p.delete(deleted, indexes);
            serializePage(s, p);
        } else {
            Index ind = getBestIndex(deleted);
            if (ind != null) {
                Vector<String> s = ind.getDeletedPages(deleted);
                for (int i = 0; i < s.size(); i++) {
                    Page p = deserializePage(s.get(i));
                    p.delete(deleted, indexes);
                    serializePage(s.get(i), p);
                }
            } else {
                for (int i = 0; i < pages.size(); i++) {
                    Page p = deserializePage(pages.get(i));
                    p.delete(deleted, indexes);
                    if (p.getRows().isEmpty()) {
                        File f = new File(pages.get(i));
                        if (f.exists()) {
                            f.delete();
                            //System.out.println("File Deleted Successfully");
                        } else
                            System.out.println("File was not there to delete it");
                        pages.remove(i);
                        i--;

                    } else
                        serializePage(pages.get(i), p);
                }
            }
        }
    }

    private Index getBestIndex(Hashtable<String, Object> deleted) {
        Index ind = null;
        int max = 0;
        for (int i = 1; i < indexes.size(); i++) {
            Index test = deserializeIndex(indexes.get(i));
            int x = commonColumns(deleted, test.getList());
            if (x > max) {
                ind = test;
                max = x;
            }
            serializeIndex(indexes.get(i), test);
        }
        return ind;
    }

    private int commonColumns(Hashtable<String, Object> deleted, String[] list) {
        int x = 0;
        for (int i = 0; i < list.length; i++) {
            if (deleted.containsKey(list[i]))
                x++;
        }
        return x;
    }

    public void update(String Prim, Hashtable<String, Object> change) throws DBAppException {
        try {
            checkcolumnexists(Name, change);
            checkerIn(Name, change);
            Index ind = deserializeIndex(indexes.get(0));
            String s = ind.getPageUpdate(Prim);
            serializeIndex(indexes.get(0), ind);
            if (s.equals("zero"))
                return;
            Page p = deserializePage(s);
            p.update(Pk, Prim, change, indexes, s);
            serializePage(s, p);

        } catch (ParseException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }

    public String getName() {
        return Name;
    }

    public Vector<String> getPages() {
        return pages;
    }

    public void createIndex(String[] list) {
        Index ind = new Index(Name, indexcounter, Pk, list);
        indexes.add("./src/main/resources/data/" + Name + "i" + indexcounter + ".ser");
        for (int i = 0; i < pages.size(); i++) {
            Page p = deserializePage(pages.get(i));
            for (int j = 0; j < p.getRows().size(); j++)
                ind.insertinBucket(pages.get(i), p.getRows().get(j));
            serializePage(pages.get(i), p);
        }
        serializeIndex("./src/main/resources/data/" + Name + "i" + indexcounter + ".ser", ind);
        indexcounter++;

    }

    public boolean checkrow(Hashtable<String, Object> row, String[] list) {
        for (int i = 0; i < list.length; i++) {
            if (!row.containsKey(list[i]))
                return false;
        }
        return true;
    }

    public void serializePage(String s, Page p) {


        try {
            FileOutputStream fileOut = new FileOutputStream(s);
            ObjectOutputStream out = new ObjectOutputStream(fileOut);
            out.writeObject(p);
            out.close();
            fileOut.close();
        } catch (IOException i) {
            i.printStackTrace();
        }
    }

    public Page deserializePage(String s) {

        FileInputStream fileIn;
        try {
            fileIn = new FileInputStream(s);
            ObjectInputStream in = new ObjectInputStream(fileIn);
            Page p = (Page) in.readObject();
            in.close();
            fileIn.close();
            return p;
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

    public Object selectFromTable(SQLTerm[] sqlTerms, String[] arrayOperators) {
        ArrayList result = new ArrayList();
        boolean flaglinear = true;
        boolean exact = false;
        Hashtable<String, Object> colNames = new Hashtable<String, Object>();
        String namesofcolumns[] = new String[sqlTerms.length];
        for (int j = 0; j < sqlTerms.length; j++) {
            colNames.put(sqlTerms[j]._strColumnName, sqlTerms[j]._objValue);
            namesofcolumns[j] = sqlTerms[j]._strColumnName;
        }
        Index d = getBestIndex(colNames);
        if (d == null) {
            for (int i = 0; i < pages.size(); i++) {
                Page p = deserializePage(pages.get(i));
                result.addAll(linearsearch(sqlTerms, arrayOperators, p));
                serializePage(pages.get(i), p);
            }

        } else {


            //exact case not always working for partial cases
            exact = true;
            flaglinear = false;
            Vector<String> m = d.getpossiblepages(sqlTerms, arrayOperators, namesofcolumns, colNames);
            for (int j = 0; j < m.size(); j++) {
                Page p = deserializePage(m.get(j));
                ArrayList semi = linearsearch(sqlTerms, arrayOperators, p);
                result.addAll(semi);

                serializePage(m.get(j), p);
            }


        }
        return result;
    }

}
