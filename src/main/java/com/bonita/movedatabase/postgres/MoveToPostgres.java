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
     * 'bjectif du program java est de créer des fichiers .sql de la forme:
     * INSERT INTO TABLE VALUES(1,'ERRER',33)
     * seulement pour les tables qui contiennent des données binaires.
     * Le nom de la table est le nome du fichier csv.
     * Quand il y a des blob il faut faire la decode
     * INSERT INTO TABLE VALUES(1,'ERRER',33, decode('2556645646456464646456456456456456456546','hex'))
     * 
     * 
     * Dataset : https://drive.google.com/drive/folders/1joDGFwHjjrT-K4JdnY__WuESBMRiW6PD?usp=sharing
     * 
     * get-content .\arch_data_instance.txt -encoding UTF8 |select-object -first 10 > output.txt
     */
    List<String> filterFiles=null;
    String encoding=null;
    String directory=null;
    File fileDirectory;
    String separatorPolicy="DETECT";
    String multilinesPolicy="REGISTER";
    boolean debug;
    
    
    public static  void main(String args[]) {
        // check options
        System.out.println("Usage : [-filter OneFile.csv] [-decoding <value>|DETECT] [-separator <value>] [-multilines ALL|NO|REGISTER] Directory");
        System.out.println("decoding : use UTF-16 or DETECT to let the program detect the correct encoding to use. Defalt is UTF-8");
        System.out.println("separator : Separator used in the CSV. If DETECT is use, then the header is used to detect the separator");
        System.out.println("multilines : A record may be store in multiple line. REGISTER (command.csv and ) by default");
        System.out.println("Version 1.1 (Dec 26 2019)");        
        int i=0;
        MoveToPostgres moveToPostgres = new MoveToPostgres();
        while (i<args.length)
        {
            if (args[i].equals("-filter")) {
                String filterFile = args.length>i+1 ? args[i+1] : null;
                moveToPostgres.filterFiles= Arrays.asList( filterFile.split(",") );
                i+=2;
            }
            // like "UTF16"

           else if (args[i].equals("-encoding")) {
               moveToPostgres.encoding = args.length>i+1 ? args[i+1] : null;
               i+=2;
           }
           else if (args[i].equals("-separator")) {
               moveToPostgres.separatorPolicy = args.length>i+1 ? args[i+1] : null;
               i+=2;
           }
           else if (args[i].equals("-multilines")) {
               moveToPostgres.multilinesPolicy = args.length>i+1 ? args[i+1] : null;
               i+=2;
           } 
           else if (args[i].equals("-debug")) {
               moveToPostgres.debug = true;
               i+=1;

           }else { 
               moveToPostgres.directory= args[ i ];
               i++;
           }
                
        }
        moveToPostgres.fileDirectory = new File(moveToPostgres.directory);
        if (! moveToPostgres.fileDirectory.exists()) {
            System.out.println("directory [" + moveToPostgres.directory + "] not exist");
            return;
        }
        for (String file : moveToPostgres.fileDirectory.list()) {
            if (moveToPostgres.filterFiles != null && ! moveToPostgres.filterFiles.contains(file))
                continue;
            moveToPostgres.manageOneFile(file);
        }

    }
    
    
    public static List<String> listMultilinesFile = Arrays.asList("command.csv", "report.csv", "process_content.csv", "job_log.csv", "connector_instance.csv");

    private void manageOneFile(String sourceFileName) {

        if (! sourceFileName.toLowerCase().endsWith(".csv")) {
            System.out.println("file [" + sourceFileName + "] does not end with .csv, ignore it");
            return;
        }
        long beginTime = System.currentTimeMillis();
        
        boolean recordsOnMultiLines = false;
        if ("ALL".equals(multilinesPolicy) || ("REGISTER".equals(multilinesPolicy) && listMultilinesFile.contains(sourceFileName.toLowerCase())))
                recordsOnMultiLines=true;
        
        int lastPos = sourceFileName.lastIndexOf(".");
        String sqlFileName = sourceFileName.substring(0, lastPos) + ".sql";
        String tableName = sourceFileName.substring(0, lastPos);
        File sourceFile = new File(fileDirectory.getAbsolutePath() + "/"+ sourceFileName);
        File sqlFile = new File(fileDirectory.getAbsolutePath() + "/"+ sqlFileName);

        FileWriter writer = null;
        BufferedWriter sqlWriter = null;

        BufferedReader br = null;
        String line = "";
        String separator = separatorPolicy; /** default **/
        System.out.println("Begin of [" + sourceFileName+"]");
        int lineCounter = 0;
        try {
            if ("DETECT".equals(encoding)) {
                encoding = detectEncoding( sourceFile );
                if ("Cp1252".equals(encoding))
                    encoding="UTF-16";
                System.out.println("  Encoding[" + encoding+"]");
                br = new BufferedReader(new InputStreamReader(new FileInputStream(sourceFile),encoding));
            }            
            else if (encoding!=null)
                br = new BufferedReader(new InputStreamReader(new FileInputStream(sourceFile),encoding));
            else
                br = new BufferedReader(new FileReader(sourceFile));
            // read the header
            String headerSt = br.readLine();
            
            if ("DETECT".equals(separatorPolicy)) {
                separator= detectSeparator( headerSt);
                System.out.println("  Separator[" + separator+"]");
            }
            String[] header = headerSt.split(separator);
               // create the output 
            writer = new FileWriter(sqlFile);
            sqlWriter = new BufferedWriter(writer);
            previousLine=null;

            while ((line = readNextLine(br, recordsOnMultiLines, separator, header.length )) != null) {
                if (lineCounter % 100 ==0) {
                    if (lineCounter % 10000 ==0 && lineCounter>0)
                        System.out.print("# "+lineCounter+" #");
                    else
                        System.out.print(".");
                    sqlWriter.flush();
                }
            
                if (debug)
                    System.out.println("Line ["+ (line.length()>30 ? line.substring(0,30)+"...":line)+"]");
                
                // use comma as separator
                String[] contentLine = line.split(separator);
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
        if (lineCounter>100)
            System.out.println("");
        System.out.println("End of [" + sourceFileName + "] manage " + lineCounter+" records in ("+(System.currentTimeMillis()-beginTime)+" ms)");
    }

    
    static String previousLine=null;
    private  String readNextLine( BufferedReader br, boolean recordsOnMultiLines, String separator, int numberExpectedField ) throws IOException {
        if (! recordsOnMultiLines)
            return br.readLine();
        // a previous line ?

        // attention, due to the previous line, we may call this method twice

        try {
            if (previousLine == null)
                previousLine=br.readLine();
        } catch(Exception e)
        {}

        
        // Ok, let's accumulate to have a first line
        
        while (true) {
            String currentLine=br.readLine();
           
            if (lineIsANewRecord( currentLine, separator, previousLine, numberExpectedField  ))
            {
                String temp = previousLine;
                previousLine=currentLine;
                return temp;
            }
            previousLine= (previousLine==null ? "" : previousLine)+currentLine;
        }
        
        // we should never be here
    }
    
    private boolean lineIsANewRecord( String currentLine, String separator, String currentBuffer, int numberExpectedField )
    {
        if (currentLine==null)
            return true;
        
        
        if (currentLine.startsWith(" "))
            return false;
        // attention, the separator may have first a \, like \| to be used correclty by the split function. In that case, we have to search without this character
        /*
        if (separator.startsWith("\\")) {
            if (currentLine.indexOf(separator.substring(1)) == -1)
                return false;
        }
        else if (currentLine.indexOf(separator) == -1)
            return false;
            */
        
        // an another use case : if the currentBuffer + currentLine <= numberExpectedField, then we considere this line is part of the current record
        // use case 
        // 1|6|getSupervisor|Get process supervisor. Use parameter key 
        // SUPERVISOR_ID_KEY|org.bonitasoft.engine.external.process.GetSupervisor|1
        String completeLine= (currentBuffer==null ? "" : currentBuffer)+currentLine;
        String[] completeLineSplit = completeLine.split(separator);
        if (completeLineSplit.length <=numberExpectedField )
            return false;
                
        return true;
    }
    /**
     * isBlob
     * Return true if we get the list in the list of known blob, else return true
     */
    public static List<String> listBlobs = Arrays.asList(
            "document#content", 
            
            "arch_data_instance#blobvalue", 
             

            "data_instance#blobvalue", 
             
            
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

    private  boolean isBlob(String tableName, String col, String value) {
        String code = tableName.toLowerCase() + "#" + col.toLowerCase();
        if (listBlobs.contains(code.toLowerCase()))
            return true;

        //if (value.length() > 1000)
        //    return true;
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
            
            "command#issystem",
            
            "qrtz_job_details#is_durable",
            "qrtz_job_details#is_nonconcurrent",
            "qrtz_job_details#is_update_data",
            "qrtz_job_details#requests_recovery",
            
            "user_#enable",
            "user_contactinfo#personal",
            
            "page#provided",
            "page#hidden",
            
            "tenant#defauttenant",
            "profile#isdefault",
            
             "profileentry#custom",
            "theme#isdefault"
            
            );

    private  boolean isBoolean(String tableName, String col, String value) {
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
    public static List<String> listColsNumeric = Arrays.asList("tenantid", "tenant_id", "id", "containerid", "intvalue", "longvalue", "doublevalue", "floatvalue", "definitionid", "archiveDate", "userid", "sourceObjectId", "pageid", "lastupdatedate","lastupdatedby");

    private  boolean isNumeric(String tableName, String col, String value) {
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
    
    
    private String detectSeparator( String headerSt)
    {
        headerSt = headerSt.toLowerCase();
        for (int i=0;i<headerSt.length();i++)
        {
            if ((headerSt.charAt( i ) >= '0' && headerSt.charAt( i ) <= '9') || (headerSt.charAt( i ) >= 'a' && headerSt.charAt( i ) <= 'z') || headerSt.charAt( i )=='_')
                continue;
            return "\\"+String.valueOf( headerSt.charAt( i ));
        }
        return ",";
    }
    
    private String detectEncoding( File file) 
    {
        try
        {
        InputStreamReader r = new InputStreamReader(new FileInputStream(file));
        return r.getEncoding();
        } catch(Exception e)
        {
            System.out.println("Can't detect the encoding "+e.toString());
        }
        return null;
    }

}
