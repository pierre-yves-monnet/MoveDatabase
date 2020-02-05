set BCP_EXPORT_SERVER="172.20.15.20"
set BCP_EXPORT_DB=bonita_db
REM set BCP_EXPORT_TABLE=configuration
set BCP_USERNAME=ebx_gasel
set BCP_PASSWORD=EBXdbPass
set BCP_OUTPUT_DIR=C:\projects\119-migrationToPostgresql\csvOutputBlob\toTreat
rem bar_resource configuration data_instance dependency document job_param page QRTZ_JOB_DETAILS QRTZ_TRIGGERS report theme
for %%x in (bar_resource configuration data_instance dependency document job_param page QRTZ_JOB_DETAILS QRTZ_TRIGGERS report theme command connector_instance job_log process_content page_mapping) do ( 
	BCP "DECLARE @colnames VARCHAR(max);SELECT @colnames = COALESCE(@colnames + '~', '') + column_name from %BCP_EXPORT_DB%.INFORMATION_SCHEMA.COLUMNS where TABLE_NAME='%%x'; select @colnames;" queryout HeadersOnly.csv -c -U%BCP_USERNAME% -P%BCP_PASSWORD% -S%BCP_EXPORT_SERVER%

	BCP %BCP_EXPORT_DB%.dbo.%%x out TableDataWithoutHeaders.csv -c -t"~" -S%BCP_EXPORT_SERVER% -U%BCP_USERNAME% -P%BCP_PASSWORD%

	copy /b HeadersOnly.csv+TableDataWithoutHeaders.csv %BCP_OUTPUT_DIR%\%%x.csv
	set BCP_EXPORT_SERVER=
	set BCP_EXPORT_DB=
	set BCP_EXPORT_TABLE=

	del HeadersOnly.csv
	del TableDataWithoutHeaders.csv
)