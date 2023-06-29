import java.io.*;
import java.util.*;

@SuppressWarnings("ALL")
public class DBApp implements DBAppInterface
{
    private Vector<Table> tables;

    public DBApp() {
        tables = new Vector<Table>();
    }

    public static void sqltermchecker(SQLTerm[] arrSQLTerms, String[] strarrOperators) throws DBAppException {
        for (int i = 0; i < arrSQLTerms.length; i++) {
            if (arrSQLTerms[i]._objValue == null || arrSQLTerms[0]._strColumnName == null ||
                    arrSQLTerms[0]._strOperator == null || arrSQLTerms[0]._strTableName == null)
                throw new DBAppException("One or more of the SQL Terms is not available");
            if (!(arrSQLTerms[i]._strOperator.equals(">") || arrSQLTerms[i]._strOperator.equals(">=") ||
                    arrSQLTerms[i]._strOperator.equals("<") || arrSQLTerms[i]._strOperator.equals("<=") ||
                    arrSQLTerms[i]._strOperator.equals("!=") || arrSQLTerms[i]._strOperator.equals("="))) {
                throw new DBAppException("One or more of the SQL Terms operations is not applicable");

            }
            String line = "";
            String splitBy = ",";
            try {
                BufferedReader br = new BufferedReader(new FileReader(
                        "src\\main\\resources\\metadata.csv"));
                br.readLine();
                boolean flag = false;
                while ((line = br.readLine()) != null) // returns a Boolean value
                {
                    String[] data = line.split(splitBy);
                    if (data[0].equals(arrSQLTerms[i]._strTableName) && data[1].equals(arrSQLTerms[i]._strColumnName)) {
                        if (data[2].equals(arrSQLTerms[i]._objValue.getClass().getCanonicalName())) {
                            flag = true;
                            break;
                        } else
                            throw new DBAppException("One of the Columns and its corresponding Data entered dont match");

                    }


                }
                if (!flag) {
                    throw new DBAppException("The Table Name or Column is not available in the metadata File");
                }

            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        //checking second parameter
        /// Operator between SQLTerm (as in strarrOperators above) are AND, OR, or XOR.
        for (int j = 0; j < strarrOperators.length; j++) {
            if (!(strarrOperators[j].equals("AND") || strarrOperators[j].equals("OR") || strarrOperators[j].equals("XOR")))
                throw new DBAppException("The String Operators are not one of (AND,OR,XOR)");
        }


    }

    public static void checktablename(String tablename) throws DBAppException {
        String line = "";
        String splitBy = ",";
        try {
            BufferedReader br = new BufferedReader(new FileReader("src\\main\\resources\\metadata.csv"));
            br.readLine();
            while ((line = br.readLine()) != null) // returns a Boolean value
            {
                String[] data = line.split(splitBy); // use comma as separator
                if (data[0].equals(tablename)) {
                    throw new DBAppException("A Table with the same name already exist");
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void init() {
        // TODO Auto-generated method stub

    }

    public void createTable(String tableName, String clusteringKey, Hashtable<String, String> colNameType,
                            Hashtable<String, String> colNameMin, Hashtable<String, String> colNameMax) throws DBAppException {
        // TODO Auto-generated method stub
        checktablename(tableName);


        String s;
        Stack<String> s1 = new Stack<String>();
        Enumeration<String> asw = colNameType.keys();
        while (asw.hasMoreElements()) {
            s = tableName + ",";
            String ColumnName = asw.nextElement();
            s += ColumnName + ",";
            s += colNameType.get(ColumnName) + ",";
            if (ColumnName.equals(clusteringKey))
                s += "True,";
            else
                s += "False,";
            s += "False,";
            s += colNameMin.get(ColumnName) + "," + colNameMax.get(ColumnName);
            s1.push(s);
            //System.out.println(s);
        }

        String newline = "\n";
        try {
            FileWriter filewriter = new FileWriter("src\\main\\resources\\metadata.csv", true);
            while (s1.isEmpty() == false) {
                filewriter.append(newline);
                filewriter.append((String) s1.pop());
            }
            filewriter.flush();
            filewriter.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        Table t = new Table(tableName, clusteringKey);
        tables.add(t);
        serializeTable(t);
    }

    @Override
    public void createIndex(String tableName, String[] columnNames) throws DBAppException {
        Table t = deserializeTable(tableName);
        t.createIndex(columnNames);
        serializeTable(t);
    }

    @Override
    public void insertIntoTable(String tableName, Hashtable<String, Object> colNameValue) throws DBAppException {
        // TODO Auto-generated method stub
        Table t = deserializeTable(tableName);
        t.insert(colNameValue);
        serializeTable(t);
    }

    @Override
    public void updateTable(String tableName, String clusteringKeyValue, Hashtable<String, Object> columnNameValue)
            throws DBAppException {
        // TODO Auto-generated method stub
        Table t = deserializeTable(tableName);
        t.update(clusteringKeyValue, columnNameValue);
        serializeTable(t);
    }

    @Override
    public void deleteFromTable(String tableName, Hashtable<String, Object> columnNameValue) throws DBAppException {
        // TODO Auto-generated method stub
        Table t = deserializeTable(tableName);
        t.delete(columnNameValue);
        serializeTable(t);
    }

    @Override
    public Iterator selectFromTable(SQLTerm[] sqlTerms, String[] arrayOperators) throws DBAppException {
        sqltermchecker(sqlTerms, arrayOperators);
        String tablename = sqlTerms[0]._strTableName;
        Table t = (Table) deserializeTable(tablename);

        Iterator resultSet = ((ArrayList) (t.selectFromTable(sqlTerms, arrayOperators))).iterator();
        while (resultSet.hasNext()) {
            System.out.println(resultSet.next());
        }
        serializeTable(t);
        return resultSet;
    }

    public void serializeTable(Table t) {
        String s = t.getName() + ".ser";

        try {
            FileOutputStream fileOut = new FileOutputStream(s);
            ObjectOutputStream out = new ObjectOutputStream(fileOut);
            out.writeObject(t);
            out.close();
            fileOut.close();
        } catch (IOException i) {
            i.printStackTrace();
        }
    }

    public Table deserializeTable(String t) {
        String s = t + ".ser";

        FileInputStream fileIn;
        try {
            fileIn = new FileInputStream(s);
            ObjectInputStream in = new ObjectInputStream(fileIn);
            Table t1 = (Table) in.readObject();
            in.close();
            fileIn.close();
            return t1;
        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            //e.printStackTrace();
            System.out.print("No page");
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return null;
    }

    public static void main(String[] args) throws DBAppException {

        DBApp app = new DBApp();

        Hashtable<String, Object> h = new Hashtable<String, Object>();
        //h.put("id", "43-0276");
        //h.put("gpa",1.12 );
        //h.put("first_name","Karim");

        //app.deleteFromTable("students",h);

        //selecting part
        SQLTerm[] arrSQLTerms;
        arrSQLTerms = new SQLTerm[1];
        arrSQLTerms[0] = new SQLTerm();
        arrSQLTerms[0]._strTableName = "students";
        arrSQLTerms[0]._strColumnName = "dob";
        arrSQLTerms[0]._strOperator = "<";
        int year = 1997;
        int month = 1;
        int day = 1;

        Date dob = new Date(year - 1900, month - 1, day);
        arrSQLTerms[0]._objValue = dob;

        String[] strarrOperators = new String[1];
        strarrOperators[0] = "OR";
        Iterator resultSet = app.selectFromTable(arrSQLTerms, strarrOperators);
    }

}

