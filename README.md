MoveDatabase
This project is used in the context of a migration of a Bonita database schema to migrate from SqlServer to Postgresql.
Not all the Bonita engine tables are used, but only the ones used by our customer XYZ, so it's not exaustive.
The jar delivered through this project "movetopostgres.jar" has to manage only a set of tables with blob data.
For other more simple table it's ok to export the data to csv files and import them with the copy program of Postgresql.
Everything is explained here:
https://bonitasoft.atlassian.net/wiki/spaces/DS/pages/1366753892/SqlServer+to+Postgresql+migration?atlOrigin=eyJpIjoiMDUwZTNmMTdmNmUyNDVkZmFmNGU2ZmM2NzE0YTUwZWIiLCJwIjoiYyJ9

The following programs are used:
For no blob data:
- generate CSV with bcpCreateCSVNoBlob.bat: it creates CSV files from SQLserver through the powershell (Consider changing the | separator because it gave some issues)
- import into Postgresql with postgresCopyDataFromCsv.bat

For Blob data:
- generate CSV with bcpCreateCsvBlobWithHeader.bat It generates the csv files that will be used
- generate .sql files (insert into... style) with movetopostgres.jar
- execute sql files into Postgresql


