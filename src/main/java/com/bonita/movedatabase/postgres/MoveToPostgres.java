package com.bonita.movedatabase.postgres;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MoveToPostgres {

    /**
     * 'bjectif du program java est de cr�er des fichiers .sql de la forme:
     * INSERT INTO TABLE VALUES(1,'ERRER',33)
     * seulement pour les tables qui contiennent des donn�es binaires.
     * Le nom de la table est le nome du fichier csv.
     * Quand il y a des blob il faut faire la decode
     * INSERT INTO TABLE VALUES(1,'ERRER',33, decode('2556645646456464646456456456456456456546','hex'))
     * 
     * 
     * Dataset : https://drive.google.com/drive/folders/1joDGFwHjjrT-K4JdnY__WuESBMRiW6PD?usp=sharing
     */

    public static  void main(String args[]) {
        // check options
        System.out.println("Usage : [-filter OneFile.csv] [-decoding UTF-16] Directory");
        int i=0;
        String filterFile=null;
        String encoding=null;
        String directory=null;
        while (i<args.length)
        {
            if (args[i].equals("-filter")) {
                filterFile = args.length>i+1 ? args[i+1] : null;
                i+=2;
            }
            // like "UTF16"

           else if (args[i].equals("-encoding")) {
               encoding = args.length>i+1 ? args[i+1] : null;
               i+=2;
           }
           else { 
               directory= args[ i ];
               i++;
           }
                
        }
        File fileDirectory = new File(directory);
        if (!fileDirectory.exists()) {
            System.out.println("directory [" + directory + "] not exist");
            return;
        }
        for (String file : fileDirectory.list()) {
            if (filterFile != null && ! filterFile.equals(file))
                continue;
            manageOneFile(fileDirectory, file, encoding);
        }

    }

    private static void manageOneFile(File directory, String sourceFileName, String encoding) {

        if (! sourceFileName.toLowerCase().endsWith(".csv")) {
            System.out.println("file [" + sourceFileName + "] does not end with .csv, ignore it");
            return;
        }

        int lastPos = sourceFileName.lastIndexOf(".");
        String sqlFileName = sourceFileName.substring(0, lastPos) + ".sql";
        String tableName = sourceFileName.substring(0, lastPos);
        File sourceFile = new File(directory.getAbsolutePath() + "/"+ sourceFileName);
        File sqlFile = new File(directory.getAbsolutePath() + "/"+ sqlFileName);

        FileWriter writer = null;
        BufferedWriter sqlWriter = null;

        BufferedReader br = null;
        String line = "";
        String cvsSplitBy = ",";
        System.out.println("Begin of [" + sourceFileName+"]");
        int lineCounter = 0;
        try {
            if (encoding!=null)
                br = new BufferedReader(new InputStreamReader(new FileInputStream(sourceFile),encoding));
            else
                br = new BufferedReader(new FileReader(sourceFile));
            // read the header
            String headerSt = br.readLine();
            String[] header = headerSt.split(cvsSplitBy);
            if (lineCounter % 100 ==0)
                System.out.print(".");
            // create the output 
            writer = new FileWriter(sqlFile);
            sqlWriter = new BufferedWriter(writer);

            while ((line = br.readLine()) != null) {

                // use comma as separator
                String[] contentLine = line.split(cvsSplitBy);
                String listColumns = "";
                String listValues = "";
                lineCounter++;
                for (int i = 0; i < header.length; i++) {
                    String col = header[i];
                    String value = contentLine.length > i ? contentLine[i] : "null";
                    if (i > 0) {
                        listColumns += ", ";
                        listValues += ", ";
                    }
                    listColumns += col;
                    if (value == null)
                        listValues += " null ";
                    else if (isBlob(tableName, col, value)) {
                        
                        value = value.replaceAll("\'", "''");
                        if (value.trim().length()==0)
                            listValues+= " null ";
                        else if (value.startsWith("<?xml version="))
                            listValues+= "'"+value+"'";
                        else
                            listValues += "decode('" + value + "','hex')";
                    }
                    else if (isBoolean(tableName, col, value))
                        listValues += (value.equals("1")? "true" : "false");
                    else if (isNumeric(tableName, col, value)) {
                        // attention, value may be a empty value (=> Transform to null then)
                        if (value.trim().length()==0)
                            listValues += "null";
                        else
                            listValues += value;
                    }
                    else {
                        value = value.replaceAll("\'", "''");
                        listValues += "'" + value + "'";
                    }

                }

                // now, process
                String sqlRequest = "insert into " + tableName + " (" + listColumns + ") values (" + listValues + ");\n";
                sqlWriter.write(sqlRequest);

            }
            sqlWriter.flush();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (writer != null) {
                try {
                    writer.flush();
                    writer.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        System.out.println("End of [" + sourceFileName + "] manage " + lineCounter);
    }

    /**
     * isBlob
     * Return true if we get the list in the list of known blob, else return true
     */
    public static List<String> listBlobs = Arrays.asList(
            "document#content", 
            
            "arch_data_instance#blobvalue", 
            "arch_data_instance#clobvalue", 

            "data_instance#blobvalue", 
            "data_instance#clobvalue", 
            
            "job_param#value_",
            
            "pdependency#value_",
            "dependency#value_", 
            "page#content",
            
            "blob_#blobvalue", 
            "configuration#resource_content",
            "report#screenshot", 
            "report#content",
            
            "theme#content",
            "theme#cssContent",
            "bar_resource#content",
            
            "tenant_resource#content",
            "icon#content"
            
            );

    private static boolean isBlob(String tableName, String col, String value) {
        String code = tableName.toLowerCase() + "#" + col.toLowerCase();
        if (listBlobs.contains(code.toLowerCase()))
            return true;

        if (value.length() > 1000)
            return true;
        return false;
    }
    /**
     * isBoolean
     * Return true if we get the list in the list of known blob, else return true
     */
    public static List<String> listBoolean = Arrays.asList(
            "document#hascontent", 
            "actor#initiator", 
            "arch_flownode_instance#terminal", 
            "arch_flownode_instance#stable",
            "arch_flownode_instance#sequential",
            "arch_flownode_instance#aborting",
            "arch_flownode_instance#triggeredbyevent",
            "arch_flownode_instance#interrupting",
            
            "arch_data_instance#transientdata",
            "arch_data_instance#booleanvalue",
            
            "data_instance#transientdata",
            "data_instance#booleanvalue",
            
            "flownode_instance#stable",
            "flownode_instance#sequential",
            "flownode_instance#aborting",
            "flownode_instance#triggeredbyevent",
            "flownode_instance#interrupting",
            "waiting_event#locked",
            "waiting_event#active",
            "message_instance#locked",
            "message_instance#handled",
            "report#provided",
            "command#issytem",
            
            "user_#enable",
            "user_contactinfo#personal",
            
            "page#provided",
            "page#hidden",
            
            "tenant#defauttenant",
            "profile#isdefault",
            
             "profileentry#custom",
            "theme#isdefault"
            
            );

    private static boolean isBoolean(String tableName, String col, String value) {
        String code = tableName.toLowerCase() + "#" + col.toLowerCase();
        if (listBoolean.contains(code.toLowerCase()))
            return true;
        return false;
    }
    /**
     * isNumeric
     * return true if the column is in the list of known numeric column, or if we can transform the value to a double
     */
    public static List<String> listNumeric = Arrays.asList("document#content");
    public static List<String> listColsNumeric = Arrays.asList("tenantid", "tenant_id", "id", "containerid", "intvalue", "longvalue", "doublevalue", "floatvalue", "definitionid", "archiveDate", "userid", "sourceObjectId");

    private static boolean isNumeric(String tableName, String col, String value) {
        if (listColsNumeric.contains(col.toLowerCase()))
            return true;
        String code = tableName.toLowerCase() + "#" + col.toLowerCase();
        if (listNumeric.contains(code))
            return true;
        // Ok, try to see if the value is a numeric
        try {
            Double valueDouble = Double.valueOf(value);
            return true;
        } catch (Exception e) {

        }
        return false;

    }
}
