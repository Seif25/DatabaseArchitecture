import java.io.*;
import java.util.Date;
import java.util.Hashtable;
import java.util.Vector;

@SuppressWarnings("ALL")
public class Index implements Serializable{
    String[] index;
    Vector<Hashtable<Integer,Object>> ranges;
    String[] list;
    String[] listtypes;
    String Name;
    int indexcounter;
    String PK;
    String Pktype;

    public Index(String Name,int indexcounter,String PK,String[] list) {
        index = new String[(int) Math.pow(10,list.length)];
        this.list = list;
        ranges = new Vector<Hashtable<Integer,Object>>(list.length);
        this.Name = Name;
        this.indexcounter = indexcounter;
        this.PK =PK;
        listtypes = new String[list.length];

        for(int i = 0 ; i<list.length;i++) {
            String Columnname = list[i];
            Hashtable<Integer,Object> h = new Hashtable<Integer,Object>();
            ranges.add(h);
            String line = "";
            String splitBy = ",";
            try {
                BufferedReader br = new BufferedReader(new FileReader("src\\main\\resources\\metadata.csv"));
                br.readLine();

                while ((line = br.readLine()) != null) // returns a Boolean value
                {
                    String[] data = line.split(splitBy);
                    if(data[0].equals(Name))
                    {
                        if(Columnname.equals(data[1]))
                        {
                            if(data[3].equals("True"))
                                Pktype = data[2];
                            data[4] ="True";
                            String min = data[5];
                            String max = data[6];
                            if(data[2].equals("java.lang.Integer")) {
                                listtypes[i] = data[2];
                                int minimum = Integer.parseInt(min);
                                int Maximum = Integer.parseInt(max);
                                int range = (Maximum-minimum)/9;
                                int step = range;
                                range += minimum;
                                for(int j = 0;j<9;j++) {
                                    h.put(j,range);
                                    range += step;
                                }
                            }
                            else if(data[2].equals("java.lang.Double")) {
                                listtypes[i] = data[2];
                                double minimum = Double.parseDouble(min);
                                double Maximum = Double.parseDouble(max);
                                double range = (Maximum-minimum)/9;
                                double step = range;
                                range += minimum;
                                for(int j = 0;j<9;j++) {
                                    h.put(j,range);
                                    range += step;
                                }
                            }
                            else if(data[2].equals("java.lang.String")) {
                                listtypes[i] = data[2];
                                String str = min;
                                for(int j=0;j<9;j++) {
                                    String z ="";
                                    for(int m = 0; m<str.length();m++) {
                                        char x = max.charAt(m);
                                        char y = min.charAt(m);
                                        int d = x-y;
                                        int range = (int)(d/9);
                                        int step = range;
                                        int rem = d%9;
                                        if((rem == 1 && j==4)
                                                || (rem ==2 &&(j == 2 || j==7))
                                                || (rem ==3 &&(j == 1 || j==4 || j==7))
                                                || (rem ==4 &&(j == 1 || j==3 || j==5 || j==7))
                                                || (rem ==5 &&(j == 1 || j==3 || j==5 || j==7 || j==9))
                                                || (rem ==6 &&(j == 1 || j==3 || j==4 || j==5 || j==7 || j==9))
                                                || (rem ==7 &&(j == 0 || j==2 || j==3 || j==4 || j==5 || j==7 || j==9))
                                                || (rem ==8 &&(j == 0 || j==1 || j==2 || j==3 || j==5 || j==7 || j==8 || j==9))
                                                || (rem ==9 &&(j == 0 || j==1 || j==2 || j==3 || j==5 || j==6 || j==7 || j==8 || j==9))) {
                                            step += 1;
                                        }
                                        z += (char)(str.charAt(m) + step);
                                    }
                                    str = z;
                                    h.put(j,z);
                                }
                            }
                            else if(data[2].equals("java.util.Date")) {
                                listtypes[i] = data[2];
                                int year = Integer.parseInt(data[5].trim().substring(0, 4));
                                int month = Integer.parseInt(data[5].trim().substring(5, 7));
                                int day = Integer.parseInt(data[5].trim().substring(8));
                                Date date1 = new Date(year - 1900, month - 1, day);
                                int year1 = Integer.parseInt(data[6].trim().substring(0, 4));
                                int month1 = Integer.parseInt(data[6].trim().substring(5, 7));
                                int day1 = Integer.parseInt(data[6].trim().substring(8));
                                Date date2 = new Date(year1 - 1900, month1 - 1, day1);
                                int year2 = (year1-year)/9;
                                int month2 = (month1 - month)/9;
                                int day2 = (day1 - day)/9;
                                Date range = new Date(year2+year - 1900, month2+month - 1, day2+day);
                                for(int j = 0;j<9;j++) {
                                    h.put(j,range);
                                    int year3 = range.getYear()+year2;
                                    int month3 = range.getMonth()+month2;
                                    int day3 = range.getDay()+day2;
                                    range = new Date(year3 , month3 , day3);
                                    if(j==7)
                                        range = date2;
                                }
                            }
                        }
                    }
                }

                br.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public void makePklast(int i) {
        for(int x=i;x<list.length-1;x++) {
            list[i] = list[i+1];
        }
        list[list.length-1] = PK;
    }

    public void insertinBucket(String filepath,Hashtable<String,Object> input) {
        int x = getIndex(input);
        String s = filepath +","+ input.get(PK);
        for(int i=0;i<list.length;i++) {
            s += ","+input.get(list[i]);
        }
        Bucket b;
        if(index[x] ==null) {
            b = new Bucket();
            b.getBucket().add(s);
            index[x] = "notempty";
        }
        else {
            b = deserializeBucket(x);
            b.insert(input,filepath,list,PK);
        }
        serializeBucket(x,b);
    }

    public int getIndex(Hashtable<String,Object> hash) {
        String index = "";
        for(int i =0;i<ranges.size();i++) {
            Hashtable<Integer,Object> range= ranges.get(i);
            if(hash.get(list[i])==null) {
                index+=9;
            }
            else {
                if(range.get(0) instanceof Integer) {
                    if((int)hash.get(list[i]) <= (int)range.get(0)) {
                        index += 0;
                    }
                    else if((int)hash.get(list[i]) > (int)range.get(7)) {
                        index +=8;
                    }
                    else {
                        int l = 0, r = 8;
                        while (l <= r) {
                            int m = l + (r - l) / 2;

                            // Check if x is present at mid
                            if ((int)hash.get(list[i]) <= (int)range.get(m) && (int)hash.get(list[i]) > (int)range.get(m-1)) {
                                index += m;
                                break;
                            }

                            // If x greater, ignore left half
                            else if ((int)hash.get(list[i]) > (int)range.get(m)) {
                                l = m + 1;
                            }

                            // If x is smaller, ignore right half
                            else  if ((int)hash.get(list[i]) <= (int)range.get(m-1)){
                                r = m - 1;
                            }

                        }
                    }
                }
                else if(range.get(0) instanceof Double ) {
                    if((double)hash.get(list[i]) <= (double)range.get(0)) {
                        index += 0;
                    }
                    else if((double)hash.get(list[i]) > (double)range.get(7)) {
                        index +=8;
                    }

                    else {
                        int l = 0, r = 8;
                        while (l <= r) {
                            int m = l + (r - l) / 2;

                            // Check if x is present at mid
                            if ((double)hash.get(list[i]) <= (double)range.get(m) && (double)hash.get(list[i]) > (double)range.get(m-1)) {
                                index += m;
                                break;
                            }

                            // If x greater, ignore left half
                            else if ((double)hash.get(list[i]) > (double)range.get(m)) {
                                l = m + 1;
                            }

                            // If x is smaller, ignore right half
                            else  if ((double)hash.get(list[i]) <= (double)range.get(m-1)){
                                r = m - 1;
                            }

                        }
                    }
                }
                else if(range.get(0) instanceof Date ) {
                    Date d = (Date) hash.get(list[i]);
                    if(d.compareTo((Date)range.get(0))<=0) {
                        index += 0;
                    }
                    else if(d.compareTo((Date)range.get(7))>0) {
                        index +=8;
                    }
                    else {
                        int l = 0, r = 8;
                        while (l <= r) {
                            int m = l + (r - l) / 2;

                            // Check if x is present at mid
                            if (d.compareTo((Date)range.get(m))<=0 && d.compareTo((Date)range.get(m-1))>0) {
                                index += m;
                                break;
                            }

                            // If x greater, ignore left half
                            else if (d.after((Date)range.get(m))) {
                                l = m + 1;
                            }

                            // If x is smaller, ignore right half
                            else  if (d.before((Date)range.get(m))){
                                r = m - 1;
                            }

                        }
                    }
                }
                else if(range.get(0) instanceof String ) {
                    String d = (String) hash.get(list[i]);
                    if(d.compareTo((String)range.get(0))<=0) {
                        index += 0;
                    }
                    else if(d.compareTo((String)range.get(7))>0) {
                        index +=8;
                    }
                    else {
                        int l = 0, r = 8;
                        while (l <= r) {
                            int m = l + (r - l) / 2;

                            // Check if x is present at mid
                            if (d.compareTo((String)range.get(m))<=0 && d.compareTo((String)range.get(m-1))>0) {
                                index += m;
                                break;
                            }

                            // If x greater, ignore left half
                            else if (d.compareTo((String)range.get(m))>0) {
                                l = m + 1;
                            }

                            // If x is smaller, ignore right half
                            else  if (d.compareTo((String)range.get(m))<=0){
                                r = m - 1;
                            }

                        }
                    }
                }
            }
        }
        return Integer.parseInt(index);
    }

    public void serializeBucket(int index,Bucket b) {

        String s = "./src/main/resources/data/"+indexcounter+Name+index+".ser";
        try {
            FileOutputStream fileOut = new FileOutputStream(s);
            ObjectOutputStream out = new ObjectOutputStream(fileOut);
            out.writeObject(b);
            out.close();
            fileOut.close();
        } catch (IOException i) {
            i.printStackTrace();
        }
    }

    public Bucket deserializeBucket(int index) {


        String s = "./src/main/resources/data/"+indexcounter+Name+index+".ser";

        FileInputStream fileIn;
        try {
            fileIn = new FileInputStream(s);
            ObjectInputStream in = new ObjectInputStream(fileIn);
            Bucket b = (Bucket) in.readObject();
            in.close();
            fileIn.close();
            return b;
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

    public String[] getList() {
        return list;
    }

    public void delete(Hashtable<String, Object> deleted) {
        int x = getIndex(deleted);
        String y = x+"";
        Vector<Integer> buckets= new Vector<Integer>();
        for(int i=0;i<index.length;i++) {
            boolean flag = true;
            for(int j=0;j<y.length();j++) {
                int z = x/ ((int)Math.pow(10.0, j));
                int m = i/ ((int)Math.pow(10.0, j));
                if(z%10 !=9 && z%10 != m%10) flag = false;
            }
            if(flag) buckets.add(i);
        }
        for(int i=0;i<buckets.size();i++) {
            if(index[buckets.get(i)] != null) {
                Bucket b = deserializeBucket(buckets.get(i));
                b.deleteFromBucket(deleted, list,PK);
                serializeBucket(buckets.get(i),b);
            }
        }
    }

    public String getPage(Hashtable<String,Object> input, int key) {
        int x = getIndex(input);
        for(int i =x;i>=0;i--) {
            if(index[i] == null)
                continue;
            else {
                Bucket b= deserializeBucket(i);
                String s = b.getPage(input.get(PK),key);
                serializeBucket(i,b);
                if(s != null) {
                    return s;
                }
            }
        }
        return "zero";
    }

    public String getPageUpdate(String prim) {
        Hashtable<String, Object> h = new Hashtable<String,Object>();
        if(Pktype.equals("java.lang.Integer"))
            h.put(PK, Integer.parseInt(prim));
        else if(Pktype.equals("java.lang.Double"))
            h.put(PK, Double.parseDouble(prim));
        else if(Pktype.equals("java.util.Date")) {
            int year = Integer.parseInt(prim.trim().substring(0, 4));
            int month = Integer.parseInt(prim.trim().substring(5, 7));
            int day = Integer.parseInt(prim.trim().substring(8));
            Date dob = new Date(year, month, day);
            h.put(PK,dob);
        }
        else
            h.put(PK, prim);
        return getPage(h,1);


    }

    public void replace(Hashtable<String, Object> news, Hashtable<String, Object> old,String filepath) {
        delete(old);
        if(news != null)
            insertinBucket(filepath,news);

    }

    public Vector<String> getDeletedPages(Hashtable<String, Object> deleted) {
        int x = getIndex(deleted);
        String y = x+"";
        Vector<Integer> buckets= new Vector<Integer>();
        for(int i=0;i<index.length;i++) {
            boolean flag = true;
            for(int j=0;j<y.length();j++) {
                int z = x/ ((int)Math.pow(10.0, j));
                int m = i/ ((int)Math.pow(10.0, j));
                if(z%10 !=9 && z%10 != m%10) flag = false;
            }
            if(flag) buckets.add(i);
        }
        Vector<String> all = new Vector<String>();
        for(int i=0;i<buckets.size();i++) {
            if(index[buckets.get(i)] != null) {
                Bucket b = deserializeBucket(buckets.get(i));
                Vector<String> news = b.getpagesdeleted(deleted, list,PK);
                serializeBucket(buckets.get(i),b);
                nodupes(all,news);
            }
        }
        return all;
    }

    private void nodupes(Vector<String> all, Vector<String> news) {
        for(int i=0;i<news.size();i++) {
            if(! all.contains(news.get(i)))
                all.add(news.get(i));
        }
    }

    public static void main(String[] args) {
        Index ind = new Index("courses",0,"date_added",new String[] {"course_id","date_added"});
        Date d = new Date(2020 -1900,12 -1,31);
        System.out.println(d);
        Hashtable<String,Object> h = new Hashtable<String,Object>();
        h.put("date_added", d);


        int x =ind.getIndex(h);
        System.out.print(x);

    }

    private int compares(Object object, String string) {
        if(object instanceof Integer) {
            if((object.equals(string))) {
                return 0;
            }
            else if((int)object>Integer.parseInt(string))
                return 1;
            else
                return -1;
        }
        else if(object instanceof Double) {
            if((double)object==Double.parseDouble(string) )
                return 0;
            else if((double)object>Double.parseDouble(string))
                return 1;
            else
                return -1;
        }
        else if(object instanceof String) {
            if(((String)""+object).compareTo(string) == 0)
                return 0;
            else if(((String)""+ object).compareTo((string)) >0) {
                return 1;
            }
            else {
                return -1;
            }
        }
        else {
            if(object.equals(string))
                return 0;
            else if(((Date) object).compareTo((Date)((Object) string)) >0)
                return 1;
            else
                return -1;
        }

    }

    public void serializePage(String s,Page p) {



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

    public Vector<String> getpossiblepages(SQLTerm[] sqlTerms, String[] arrayOperators, String[] namesofcolumns,
                                           Hashtable <String,Object> hash	) {


        Vector<Integer> result = getIndexmodified(sqlTerms,arrayOperators);
        Vector<String> pages = new Vector<String>();
        Vector<String> temp = new Vector<String>();
        for(int i=0;i<result.size();i++) {
            if(index[result.get(i)] !=null) {
                Bucket b = deserializeBucket(result.get(i));
                temp=b.getSelectPages(sqlTerms,arrayOperators,list,listtypes);
                nodupes(pages,temp);
                serializeBucket(result.get(i),b);
            }
        }

        return pages;
    }

    public Vector<Integer> getIndexmodified(SQLTerm[] sqlTerms,String[] arrayOperators) {
        Vector<Integer> result = new Vector<Integer>();
        boolean firstquery = true;
        String x = "";
        for(int i=0;i<sqlTerms.length;i++) {
            Vector<Integer> semiresult = new Vector<Integer>();
            for(int j =0;j<ranges.size();j++) {

                Hashtable<Integer,Object> range= ranges.get(j);
                if(! sqlTerms[i]._strColumnName.equals(list[j])) {
                    x+=9;
                }
                else {
                    if(range.get(0) instanceof Integer) {
                        if((int) (sqlTerms[i]._objValue) <= (int)range.get(0)) {
                            x += 0;
                        }
                        else if((int)sqlTerms[i]._objValue > (int)range.get(7)) {
                            x +=8;
                        }
                        else {
                            int l = 0, r = 8;
                            while (l <= r) {
                                int m = l + (r - l) / 2;

                                // Check if x is present at mid
                                if ((int)sqlTerms[i]._objValue  <= (int)range.get(m) && (int)sqlTerms[i]._objValue  > (int)range.get(m-1)) {
                                    x += m;
                                    break;
                                }

                                // If x greater, ignore left half
                                else if ((int)sqlTerms[i]._objValue  > (int)range.get(m)) {
                                    l = m + 1;
                                }

                                // If x is smaller, ignore right half
                                else  if ((int)sqlTerms[i]._objValue  <= (int)range.get(m-1)){
                                    r = m - 1;
                                }


                            }
                        }
                    }
                    else if(range.get(0) instanceof Double ) {
                        if((double)(sqlTerms[i]._objValue)  <= (double)range.get(0)) {
                            x += 0;
                        }
                        else if((double)sqlTerms[i]._objValue  > (double)range.get(7)) {
                            x +=8;
                        }

                        else {
                            int l = 0, r = 8;
                            while (l <= r) {
                                int m = l + (r - l) / 2;

                                // Check if x is present at mid
                                if ((double)sqlTerms[i]._objValue  <= (double)range.get(m) && (double)sqlTerms[i]._objValue  > (double)range.get(m-1)) {
                                    x += m;
                                    break;
                                }

                                // If x greater, ignore left half
                                else if ((double)sqlTerms[i]._objValue  > (double)range.get(m)) {
                                    l = m + 1;
                                }

                                // If x is smaller, ignore right half
                                else  if ((double)sqlTerms[i]._objValue  <= (double)range.get(m-1)){
                                    r = m - 1;
                                }

                            }
                        }
                    }
                    else if(range.get(0) instanceof Date ) {
                        Date d = (Date) sqlTerms[i]._objValue ;
                        if(d.compareTo((Date)range.get(0))<=0) {
                            x += 0;
                        }
                        else if(d.compareTo((Date)range.get(7))>0) {
                            x +=8;
                        }
                        else {
                            int l = 0, r = 8;
                            while (l <= r) {
                                int m = l + (r - l) / 2;

                                // Check if x is present at mid
                                if (d.compareTo((Date)range.get(m))<=0 && d.compareTo((Date)range.get(m-1))>0) {
                                    x += m;
                                    break;
                                }

                                // If x greater, ignore left half
                                else if (d.after((Date)range.get(m))) {
                                    l = m + 1;
                                }

                                // If x is smaller, ignore right half
                                else  if (d.before((Date)range.get(m))){
                                    r = m - 1;
                                }

                            }
                        }
                    }
                    else if(range.get(0) instanceof String ) {
                        String d = (String) sqlTerms[i]._objValue ;
                        if(d.compareTo((String)range.get(0))<=0) {
                            x += 0;
                        }
                        else if(d.compareTo((String)range.get(7))>0) {
                            x +=8;
                        }
                        else {
                            int l = 0, r = 8;
                            while (l <= r) {
                                int m = l + (r - l) / 2;

                                // Check if x is present at mid
                                if (d.compareTo((String)range.get(m))<=0 && d.compareTo((String)range.get(m-1))>0) {
                                    x += m;
                                    break;
                                }

                                // If x greater, ignore left half
                                else if (d.compareTo((String)range.get(m))>0) {
                                    l = m + 1;
                                }

                                // If x is smaller, ignore right half
                                else  if (d.compareTo((String)range.get(m))<=0){
                                    r = m - 1;
                                }

                            }
                        }
                    }
                }
                int number= Integer.parseInt(x);
                for(int k=0;k<index.length;k++) {
                    boolean flag = true;
                    for(int s=0;s<x.length();s++) {
                        int z = number/ ((int)Math.pow(10.0, s));
                        int m = k/ ((int)Math.pow(10.0, s));
                        if(z%10 !=9) {
                            switch(sqlTerms[i]._strOperator)
                            {
                                case (">") :    if(m%10 < z%10 ) flag = false;  ; break;
                                case (">=") :     if(m%10 < z%10 ) flag = false;     ; break;
                                case ("<") :    if(m%10 > z%10 ) flag = false;    ; break;
                                case ("<=") :     if(m%10 > z%10 ) flag = false;     ; break;

                                case ("=") :     if(m%10 != z%10 ) flag = false; ; break;

                            }
                        }
                    }
                    if(firstquery) {
                        if(flag) result.add(k);
                    }
                    else {
                        if(flag) semiresult.add(k);
                        switch(arrayOperators[i-1])
                        {
                            case "AND" :result = intersection(result,semiresult);break;
                            case "OR" :result= union(result,semiresult)  ;break;
                            case "XOR" :result= xor(result,semiresult)  ;break;

                        }
                    }
                }

            }


            firstquery = false;
            semiresult.removeAllElements();

        }
        return result;
    }

    public static Vector<Integer> intersection(Vector<Integer> list1,Vector<Integer> list2) {
        Vector<Integer> result = new Vector<Integer>();

        for (int i = 0;i<list1.size();i++) {
            if(list2.contains(list1.get(i))) {
                result.add(list1.get(i));
            }
        }
        // System.out.println(result.size());
        return result;
    }

    public static Vector<Integer> xor(Vector<Integer> list1,Vector<Integer> list2) {
        Vector<Integer> result = new Vector<Integer>();
        result.addAll(list1);
        for (int i = 0;i<list2.size();i++) {
            if(list1.contains(list2.get(i))) {
                result.remove(list2.get(i));
            }
            else
            {
                result.add(list2.get(i));
            }
        }
        // System.out.println(result.size());
        return result;
    }

    public static Vector<Integer> union(Vector<Integer> list1,Vector<Integer> list2) {
        Vector<Integer> result = new Vector<Integer>();
        result.addAll(list1);

        for (int i = 0;i<list2.size();i++) {
            if(!result.contains(list2.get(i))) {
                result.add(list2.get(i));
            }
        }

        return result;
    }

}
