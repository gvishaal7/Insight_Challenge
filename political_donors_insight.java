import java.io.*;
import java.text.*;
import java.util.*;

public class political_donors_insight {

    //a hashmap to store all the  contributed amounts(in a list) of all user-zip pair
    private static Map<String,ArrayList<Integer>> zip_map_median = new HashMap<>();
    //a hashmap to store the total amount of all user-zip pair
    private static Map<String,Integer> zip_map_amt = new HashMap<>();
    //a hashmap to store all the contributed amounts(in a list) of all user-date pair
    private static Map<String,ArrayList<Integer>> date_map_median = new HashMap<>();
    //a hashmap to store the total amount of all user-date pair
    private static Map<String,Integer> date_map_amt = new HashMap<>();
    
    public static void main(String[] args) {
        
        BufferedReader input = null;
        try
        {
            String file_location = args[0]; //input file location
            input = new BufferedReader(new FileReader(new File(file_location))); //creates a buffer to read the input file
            String new_line = "";
            while((new_line=input.readLine())!=null){
                String[] values = new_line.split("\\|"); //splits the given string at every '|'
                ArrayList<String> record = new ArrayList<String>();
                record.add(values[0]); //CMTE_ID
                record.add(values[10]); //ZIP_CODE
                record.add(values[13]); //TRANSACTION_DT
                record.add(values[14]); //TRANSACTION_AMT
                record.add(values[15]); //OTHER_ID
                condition_check(record); //validates the input
                String check_1 = record.get(5); 
                String check_2 = record.get(6); //contains the element that is invalid
                if(check_1.equalsIgnoreCase("true") && !check_2.equalsIgnoreCase("bzbd")) { //checks if the input line has all the necessary fields
                    if(!check_2.equalsIgnoreCase("bz")) { //bz = bugged zip_code. if the zip code is valid, the input line is further processed 
                        median_by_zip(record); 
                    }
                    if(!check_2.equalsIgnoreCase("bd")) { //bd = bugged date. if the date is valid, the input line is further processed
                        median_by_date(record);
                        
                    }
                }   
            }                      
            process_date_file(); //since the data is not "streamed" for median by date, it is processed after reading all the lines of the input file
        } catch(IOException e){
            e.printStackTrace();
        }
        finally {
            try {
                if(input!=null)
                {
                    input.close();
                }
            }
            catch(IOException e){
                e.printStackTrace();
            }
        }        
    }       
    
    //function to process the user-date hashmap
    public static void process_date_file() {
        Set key_set = date_map_median.keySet(); 
        Iterator key_set_iterator = key_set.iterator();
        String[] keys = new String[key_set.size()]; //new array of strings for storing the keys and sorting them
        int index=0;
        while(key_set_iterator.hasNext()) {
            keys[index]= (String)key_set_iterator.next();
            index++;
        }
        Arrays.sort(keys); //sorts the keys alphabatically by recipient and chronologically by date
        SimpleDateFormat sdf = new SimpleDateFormat("MMddyyyy");
        Calendar calendar = Calendar.getInstance();
        for(int key_index=0;key_index<index;key_index++) {
            double median = 0;
            String curr_key = keys[key_index];
            ArrayList<Integer> amt_list = date_map_median.get(curr_key);
            //since the date stored is in milliseconds, it is converted to the required format for the output file
            String date_in_milli = curr_key.substring(curr_key.indexOf("|")+1,curr_key.length()); 
            calendar.setTimeInMillis(Long.parseLong(date_in_milli)); 
            String date = sdf.format(calendar.getTime());
            String cmte_id = curr_key.substring(0,curr_key.indexOf("|"));
            int total = date_map_amt.get(curr_key);
            int list_size = amt_list.size();
            if(list_size==1) {
                median = amt_list.get(0);
            }
            if(list_size%2 != 0) {
                median = amt_list.get(list_size/2);    
            }
            else {
                median = (amt_list.get(list_size/2)+amt_list.get((list_size/2)-1))/2.0;
            }
            median = Math.ceil(median); //rouding off the decimal according to the standards
            int med_int = (int)median;
            //line which will be written onto the output file
            String write_line =cmte_id+"|"+date+"|"+med_int+"|"+list_size+"|"+total; 
            write_to_file(write_line,"date"); 
        }       
    }
    
    //function to process the input file based on user-zip
    public static void median_by_zip(ArrayList<String> record) {
        //key = CMTE_ID|ZIP_CODE 
        //eg: C00629618|90017
        String zip_key = record.get(0)+"|"+record.get(1);
        int running_total = 0;
        double running_median = 0;
        int list_size = 0;
        if(zip_map_median.containsKey(zip_key)) { //checks if the current key exists in the hash map
            ArrayList<Integer> cont_list = zip_map_median.get(zip_key);
            int curr_amt = Integer.parseInt(record.get(3));
            //the current amount is added to the list in a position which keeps the list sorted in ascending order
            //thereby reducing the time taken to sort, while finding the median
            list_size = cont_list.size();
            if(list_size==1) { //if the list size is 1, the current value is compared to the existing value and added to the list in the appropriate position
                if(curr_amt > cont_list.get(0)) {
                    cont_list.add(curr_amt);
                }
                else {
                    cont_list.add(0, curr_amt);
                }
            }
            else {
                int curr_list_ele = cont_list.get(0);
                int i=0;
                while(curr_amt > curr_list_ele) { //runs until the current amount is greater than the current element of the list
                    i++;
                    curr_list_ele = cont_list.get(i);
                }
                cont_list.add(i,curr_amt);
            }
            list_size++;
            zip_map_median.replace(zip_key, cont_list); //replaces the existing list with the new modified list
            running_total = zip_map_amt.get(zip_key)+Integer.parseInt(record.get(3));
            zip_map_amt.replace(zip_key, running_total); //replaces the existing total with the new total
            if(list_size%2 != 0) {
                running_median = cont_list.get(list_size/2);    
            }
            else {
                running_median = (cont_list.get(list_size/2)+cont_list.get((list_size/2)-1))/2.0;
            }
        }
        else {
            //if the hashmap does not contain the key, it creates a new key and stores a list with only 1 element for that key
            ArrayList<Integer> cont_list = new ArrayList<>();
            cont_list.add(Integer.parseInt(record.get(3)));
            zip_map_median.put(zip_key, cont_list); //adds a new key-list pair to the map
            running_total = Integer.parseInt(record.get(3));
            zip_map_amt.put(zip_key, Integer.parseInt(record.get(3))); //adds a new key-value pair to the map
            running_median = Double.parseDouble(record.get(3));
            list_size++;
        }
        running_median = Math.ceil(running_median); //rouding off the decimal according to the standards
        int run_med = (int)running_median;
        //line which will be written onto the output file
        String write_line = record.get(0)+"|"+record.get(1)+"|"+run_med+"|"+list_size+"|"+running_total; 
        write_to_file(write_line,"zip");
    }
    
    //function to write the lines to the corresponding output files
    public static void write_to_file(String output_string, String file_type) {
        String file_name = "medianvals_by_date.txt";
        if(file_type.equalsIgnoreCase("zip")) {
            file_name = "medianvals_by_zip.txt";
        }
        BufferedWriter buffered_writer = null;
        FileWriter file_writer = null;
        try {
            file_writer = new FileWriter(new File("../output/"+file_name),true);
            buffered_writer = new BufferedWriter(file_writer);
            buffered_writer.write(output_string+"\n");
        } catch(IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if(buffered_writer != null) {
                    buffered_writer.close();
                }
                if(file_writer != null) {
                    file_writer.close();
                }                
            } catch(IOException e) {
                e.printStackTrace();
            }
        }
    }
    
    //function that processes each line of the input file and stores the amount in the map
    public static void median_by_date(ArrayList<String> record) {
        SimpleDateFormat sdf = new SimpleDateFormat("MMddyyyy");
        Date new_date = new Date();
        try {
            new_date = sdf.parse(record.get(2));
        } catch(ParseException e) {
            e.printStackTrace();
        }
        long time = new_date.getTime(); //converts the given time into milliseconds for easier processing
        //key = CMTE_ID|DATE(in milliseconds)
        //eg: C00629618|1488355200000
        String date_key = record.get(0)+"|"+String.valueOf(time);
        int running_total = 0;
        if(date_map_median.containsKey(date_key)) {
            ArrayList<Integer> cont_list = date_map_median.get(date_key);
            int curr_amt = Integer.parseInt(record.get(3));
            //the current amount is added to the list in a position which keeps the list sorted in ascending order
            //thereby reducing the time taken to sort, while finding the median
            int list_size = cont_list.size();
            if(list_size==1) {
                if(curr_amt > cont_list.get(0)) { //if the list size is 1, the current value is compared to the existing value and added to the list in the appropriate position
                    cont_list.add(curr_amt);
                }
                else {
                    cont_list.add(0, curr_amt);
                }
            }
            else {
                int curr_list_ele = cont_list.get(0);
                int i=0;
                while(curr_amt > curr_list_ele) { //runs until the current amount is greater than the current element of the list
                    i++;
                    curr_list_ele = cont_list.get(i);
                }
                cont_list.add(i,curr_amt); 
            }
            date_map_median.replace(date_key, cont_list); //replaces the existing list with the new modified list
            running_total = date_map_amt.get(date_key)+Integer.parseInt(record.get(3));
            date_map_amt.replace(date_key, running_total); //replaces the existing total with the new total
        }
        else {
            //if the hashmap does not contain the key, it creates a new key and stores a list with only 1 element for that key
            ArrayList<Integer> cont_list = new ArrayList<>();
            cont_list.add(Integer.parseInt(record.get(3)));
            date_map_median.put(date_key, cont_list); //adds a new key-list pair to the map
            date_map_amt.put(date_key, Integer.parseInt(record.get(3))); //adds a new key-value pair to the map
        }        
    }
    
    //function to all the conditions the input file must pass
    public static void condition_check(ArrayList<String> record) { 
        String cmte_id = record.get(0);
        String zip_code = record.get(1);
        String t_dt = record.get(2);
        String t_amt = record.get(3);
        String o_id = record.get(4);
        if(cmte_id == null || t_amt == null || cmte_id.isEmpty() || t_amt.isEmpty() || t_amt.matches(".*[a-zA-Z]+.*") || !o_id.isEmpty()) {
            //checks if cmte_id is empty
            //transaction_amt is empty
            //transaction_amt contains any non-numerical value
            //other_id is not empty
            //if anyone of the above condition is satisfied, then the input line is discarded
            record.add("false");
        }
        else {
            record.add("true");
        }
        String comment = "";
        if(zip_code.length()>=5 && !zip_code.matches(".*[A-Za-z]+.*")){
            //checks if the zip is atleast has 5 characters and they are numeric           
            //if the above condition is satisfied, then it is trimed to 5 characters
            record.set(1, zip_code.substring(0, 5));
        }
        else {
            //if the above condition is not satisfied, then it is flagged against processing for median by zip
            comment += "bz";
        }
        SimpleDateFormat sdf = new SimpleDateFormat("MMddyyyy");
        sdf.setLenient(false); //a "switch" to turn on strick parsing, i.e. the date must match the given standard
        try {
            Date date = sdf.parse(t_dt);
        }catch(ParseException e) { //if it doesn't match, it is flagged against processing for median by date
            comment += "bd";            
        }
        record.add(comment);
    }
}
