# Miva ODBC Connector

This project allows you to build an ODBC connector to work with the MivaVM.

## Build
This solution was built using Visual Studio 2012.  

You will need to copy the following [API](https://www.miva.com/support/downloads) files into the local directory:

1. mivapi.h
1. MivAPI.lib

Upon build completion you can add **MVDODBC.DLL** to your list of valid databases in either IIS / Mia.

## Testing
Download and configure an ODBC connector, such as MySQL's connector located at [https://dev.mysql.com/downloads/connector/odbc/](https://dev.mysql.com/downloads/connector/odbc/).  Setup a connection to a database server and then you can begin testing with a simple script such as:

    <MIVA STANDARDOUTPUTLEVEL = "">
    <MvOPEN NAME = "Test" DATABASE = "ODBC_Connection_Name" TYPE = "ODBC">
    <MvOPENVIEW NAME = "Test" VIEW = "Test" QUERY = "SELECT CURRENT_TIMESTAMP() AS timestamp">
    <MvEVAL EXPR = "{ 'The current time is ' $ Test.d.timestamp }">
    <MvCLOSEVIEW NAME = "Test" VIEW = "Test">
    <MvCLOSE NAME = "Test">
