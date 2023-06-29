import java.io.BufferedReader;
import java.io.FileReader;
@SuppressWarnings("ALL")
public class SQLTerm {

    public String _strTableName;
    public String _strColumnName;
    public String _strOperator;
    public Object _objValue;

    public SQLTerm() {
    }

    public SQLTerm(String _strTableName, String _strColumnName, String _strOperator, Object _objValue) {
        this._strTableName = _strTableName;
        this._strColumnName = _strColumnName;
        this._strOperator = _strOperator;
        this._objValue = _objValue;
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
                        "src\\metadata.csv"));
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
        for (int j = 0; j < strarrOperators.length; j++) {
            if (!(strarrOperators[j].equals("AND") || strarrOperators[j].equals("OR") || strarrOperators[j].equals("XOR")))
                throw new DBAppException("The String Operators are not one of (AND,OR,XOR)");
        }

    }

    public static void main(String[] args) {

    }

}
