#include "mivapi.h"

/*
 * This file and the source codes contained herein are the property of
 * Miva, Inc.  Use of this file is restricted to the specific terms and
 * conditions in the License Agreement associated with this file.  Distribution
 * of this file or portions of this file for uses not covered by the License
 * Agreement is not allowed without a written agreement signed by an officer of
 * Miva, Inc.
 *
 * Copyright 1998-2019 Miva, Inc.  All rights reserved.
 * http://www.miva.com
 */

#include <windows.h>
#include <sql.h>
#include <sqlext.h>
#include <stdio.h>
#include <math.h>
#include <errno.h>

static HINSTANCE hODBCInstance = NULL;

/*
 * ODBCDatabase
 */

typedef struct _ODBCDatabase
{
	mvDatabase	db;

	SQLHENV		hEnv;
	SQLHDBC		hDBC;

	mvFile		log;

	int			autocommit;
	int			truncate;
	int			forwardonly;

	int			in_transaction;

	char		error[ 1024 ];
} ODBCDatabase;

/*
 * ODBCParameter
 */

typedef struct _ODBCParameter
{
	int		data_integer;
	double	data_double;
	char	*data_string;
	int		data_string_length;

	SDWORD	cbData;
} ODBCParameter;

 /*
 * ODBCDatabaseVariable
 */

typedef enum _ODBCDatabaseVariableType
{
	ODBC_INTEGER,
	ODBC_DOUBLE,
	ODBC_STRING,
	ODBC_BLOB
} ODBCDatabaseVariableType;

typedef struct _ODBCDatabaseVariable
{
	ODBCDatabaseVariableType	type;
	int							column;

	int							data_integer;
	double						data_double;
	
	char						*data_string;
	SDWORD						data_string_size;
	SDWORD						cbData;

	SQLHSTMT					data_blob_stmt;
	int							data_blob_col;
} ODBCDatabaseVariable;

/*
 * ODBCDatabaseView
 */

typedef struct _ODBCDatabaseView
{
	ODBCDatabase					*db;

	SQLHSTMT						hSTMT;
	
	int								forwardonly;

	struct _ODBCDatabaseVariable	*recno;
	struct _ODBCDatabaseVariable	*eof;
	struct _ODBCDatabaseVariable	*deleted;
} ODBCDatabaseView;

/*
 * Logging
 */

void odbc_log( ODBCDatabase *db, const char *format, ... )
{
	int length;
	va_list args;
	char buffer[ 8192 ];

	if ( db->log != NULL )
	{
	    va_start( args, format );
		length = vsprintf( buffer, format, args );
		va_end( args );

		mvFile_Write( db->log, buffer, length );
	}
}

void odbc_log_data( ODBCDatabase *db, const char *buffer, int length )
{
	if ( db->log )
	{
		mvFile_Write( db->log, buffer, length );
		mvFile_Write( db->log, "\n", strlen( "\n" ) );
	}
}

/*
 * odbc_error
 */

int odbc_error( ODBCDatabase *db, const char *prefix, SQLHANDLE handle, SQLSMALLINT handle_type )
{
	int remaining;
	SQLINTEGER native;
	char state[ 6 ] ;
	char text[ 2048 ];
	SQLSMALLINT text_length;
	SQLSMALLINT index;

	strcpy( db->error, prefix );
	remaining = sizeof( db->error ) - strlen( db->error ) - 1;

	index = 1;
	if ( SQLGetDiagRec( handle_type, handle, index, state, &native, text, sizeof( text ), &text_length ) == SQL_SUCCESS )
	{
		do
		{
			if ( remaining - ( strlen( state ) + 2 ) > 0 )
			{
				strcat( db->error, state );
				strcat( db->error, ": " );

				remaining -= strlen( state ) + 2;
			}

			if ( ( remaining - text_length ) > 0 )
			{
				strcat( db->error, text );
				remaining -= text_length;
			}
		
			index++;
		} while ( SQLGetDiagRec( handle_type, handle, index, state, &native, text, sizeof( text ), &text_length ) == SQL_SUCCESS );
	}
	else if ( ( handle_type == SQL_HANDLE_STMT ) &&
			  ( SQLError( db->hEnv, db->hDBC, ( SQLHSTMT ) handle, state, &native, text, sizeof( text ), &text_length ) == SQL_SUCCESS ) )
	{
		if ( remaining - ( strlen( state ) + 2 ) > 0 )
		{
			strcat( db->error, state );
			strcat( db->error, ": " );

			remaining -= strlen( state ) + 2;
		}

		if ( ( remaining - text_length ) > 0 )
		{
			strcat( db->error, text );
			remaining -= text_length;
		}
	}
	else
	{
		strcat( db->error, "Unknown error" );
	}

	odbc_log( db , "*** Error: " ) ;
	odbc_log_data( db, db->error, strlen( db->error ) ) ;

	return 0;
}

/*
 * odbc_execute
 */

int odbc_execute( ODBCDatabase *db, SQLHSTMT hSTMT, mvVariableList input )
{
	SQLRETURN retcode;
	SQLPOINTER pToken;
	SQLSMALLINT	datatype;
	SQLUINTEGER	column_size;
	SQLSMALLINT digits;
	SQLSMALLINT nullable;
	SQLSMALLINT	bind_count;
	mvVariable variable;
	const char *value_string;
	int value_string_length;
	int param, numparams;
	ODBCParameter *parameter_data;

	numparams		= mvVariableList_Entries( input );
	parameter_data	= ( ODBCParameter * ) mvProgram_Allocate( NULL, sizeof( ODBCParameter ) * numparams );
	memset( parameter_data, 0, sizeof( ODBCParameter ) * numparams );

	if ( SQLNumParams( hSTMT, &bind_count ) != SQL_SUCCESS )
	{	
		odbc_error( db, "SQLNumParams: ", hSTMT, SQL_HANDLE_STMT );
		goto error;
	}

	if ( bind_count != numparams ) 
	{
		odbc_log( db, "*** Input parameter count mismatch: Found %d, expected %d\n", numparams, bind_count );
		sprintf( db->error, "Input parameter count mismatch: Found %d, expected %d", numparams, bind_count );
		goto error;
	}
	
	for ( param = 0, variable = mvVariableList_First( input ); variable; param++, variable = mvVariableList_Next( input ) )
	{
		datatype	= 0;
		column_size	= 0;
		digits		= 0;
		nullable	= 0;

		if ( SQLDescribeParam( hSTMT, param + 1, &datatype, &column_size, &digits, &nullable ) != SQL_SUCCESS )
		{
			odbc_log( db, "+++ SQLDescribeParam for parameter %d failed, defaulting to character bind\n", param + 1 );

				datatype	= SQL_CHAR;
			column_size	= -1;
		}

		odbc_log( db, "--- Parameter %d: datatype = %d, column_size = %d, digits = %d, nullable = %d\n",
				  param + 1,
				  datatype,
				  column_size,
				  digits,
				  nullable );

		switch ( datatype )
		{
			case SQL_LONGVARCHAR :
			case SQL_LONGVARBINARY :
			{
				value_string	= mvVariable_Value( variable, &value_string_length );

				parameter_data[ param ].data_string			= ( char * ) mvProgram_Allocate( NULL, value_string_length );
				parameter_data[ param ].data_string_length	= value_string_length;
				parameter_data[ param ].cbData				= SQL_LEN_DATA_AT_EXEC( 0 );

				memcpy( parameter_data[ param ].data_string, value_string, value_string_length );

				if ( SQLBindParameter( hSTMT, param + 1, SQL_PARAM_INPUT, SQL_C_BINARY, datatype,
									   0, 0, ( SQLPOINTER ) param, 0, &parameter_data[ param ].cbData ) == SQL_ERROR )
				{
					odbc_error( db, "SQLBindParameter: ", hSTMT, SQL_HANDLE_STMT );
					goto error;
				}

				break;
			}
			case SQL_BIGINT :
			case SQL_TINYINT :
			case SQL_SMALLINT :
			case SQL_INTEGER :
			{
				parameter_data[ param ].data_integer	= mvVariable_Value_Integer( variable );
				parameter_data[ param ].cbData			= sizeof( int );

				odbc_log( db, "+++ Parameter %d value (integer): %d\n", param + 1, parameter_data[ param ].data_integer );

				if ( SQLBindParameter( hSTMT, param + 1, SQL_PARAM_INPUT, SQL_C_SLONG, datatype, 0, 0,
									   &parameter_data[ param ].data_integer, 0,
									   &parameter_data[ param ].cbData ) == SQL_ERROR )
				{
					odbc_error( db, "SQLBindParameter: ", hSTMT, SQL_HANDLE_STMT );
					goto error;
				}

				break;
			}
			case SQL_BIT :
			{
				parameter_data[ param ].data_integer	= mvVariable_Value_Integer( variable ) ? 1 : 0;
				parameter_data[ param ].cbData			= sizeof( int );

				odbc_log( db, "+++ Parameter %d value (integer): %d\n", param + 1, parameter_data[ param ].data_integer );

				if ( SQLBindParameter( hSTMT, param + 1, SQL_PARAM_INPUT, SQL_C_SLONG, datatype, 0, 0,
									   &parameter_data[ param ].data_integer, 0,
									   &parameter_data[ param ].cbData ) == SQL_ERROR )
				{
					odbc_error( db, "SQLBindParameter: ", hSTMT, SQL_HANDLE_STMT );
					goto error;
				}

				break;
			}
			case SQL_NUMERIC :
			case SQL_DECIMAL :
			case SQL_REAL :
			case SQL_FLOAT :
			case SQL_DOUBLE :
			{
				parameter_data[ param ].data_double		= mvVariable_Value_Double( variable );
				parameter_data[ param ].cbData			= sizeof( double );

				odbc_log( db, "+++ Parameter %d value (double): %f\n", param + 1, parameter_data[ param ].data_double );

				if ( SQLBindParameter( hSTMT, param + 1, SQL_PARAM_INPUT, SQL_C_DOUBLE, datatype,
									   column_size, digits,
									   &parameter_data[ param ].data_double, 0,
									   &parameter_data[ param ].cbData ) == SQL_ERROR )
				{
					odbc_error( db, "SQLBindParameter: ", hSTMT, SQL_HANDLE_STMT );
					goto error;
				}

				break;
			}
			case SQL_CHAR :
			case SQL_VARCHAR :
			default :
			{
				value_string						= mvVariable_Value( variable, &value_string_length );
				parameter_data[ param ].data_string	= ( char * ) mvProgram_Allocate( NULL, value_string_length );
				
				if ( db->truncate && ( column_size != -1 ) && ( value_string_length > ( int ) column_size ) )
				{
					parameter_data[ param ].cbData		= column_size;
				}
				else
				{
					parameter_data[ param ].cbData		= value_string_length;
				}

				memcpy( parameter_data[ param ].data_string, value_string, value_string_length );

				odbc_log( db, "+++ Parameter %d value (string): length = %d, cbData = %d, data = '%.*s'\n",
						  param + 1, 
						  value_string_length,
						  parameter_data[ param ].cbData,
						  parameter_data[ param ].cbData < 4096 ? parameter_data[ param ].cbData : 4096,
						  parameter_data[ param ].data_string );

				if ( SQLBindParameter( hSTMT, param + 1, SQL_PARAM_INPUT, SQL_C_CHAR, datatype, 0, 0,
									   parameter_data[ param ].data_string, parameter_data[ param ].cbData,
									   &parameter_data[ param ].cbData ) == SQL_ERROR )
				{
					odbc_error( db, "SQLBindParameter: ", hSTMT, SQL_HANDLE_STMT );
					goto error;
				}

				break;
			}
		}	
	}

	if ( ( retcode = SQLExecute( hSTMT ) ) == SQL_ERROR )
	{
		odbc_error( db, "SQLExecute: ", hSTMT, SQL_HANDLE_STMT );
		goto error;
	}

	while ( retcode == SQL_NEED_DATA )
	{
		if ( ( retcode = SQLParamData( hSTMT, &pToken ) ) == SQL_NEED_DATA )
		{
			param = ( int ) pToken;

			odbc_log( db, "+++ Parameter %d value (string at exec): length = %d, data = '%.*s'\n",
					  param + 1,
					  parameter_data[ param ].data_string_length,
					  parameter_data[ param ].data_string_length < 4096 ? parameter_data[ param ].data_string_length : 4096,
					  parameter_data[ param ].data_string );
			
			if ( SQLPutData( hSTMT,
							 parameter_data[ param ].data_string,
							 parameter_data[ param ].data_string_length ) != SQL_SUCCESS )
			{
				odbc_error( db, "SQLPutData: ", hSTMT, SQL_HANDLE_STMT );
				//goto error;
			}
		}
		else if ( retcode == SQL_ERROR )
		{
			odbc_error( db, "SQLParamData: ", hSTMT, SQL_HANDLE_STMT );
			goto error;
		}
	}

	for ( param = 0; param < numparams; param++ )
	{
		if ( parameter_data[ param ].data_string )	mvProgram_Free( NULL, parameter_data[ param ].data_string );
	}

	mvProgram_Free( NULL, parameter_data );
	return 1;

error:
	
	for ( param = 0; param < numparams; param++ )
	{
		if ( parameter_data[ param ].data_string )	mvProgram_Free( NULL, parameter_data[ param ].data_string );
	}

	mvProgram_Free( NULL, parameter_data );
	return 0;
}

/*
 * odbc_bind_columns
 */

int odbc_bind_columns( mvDatabaseView view, ODBCDatabaseView *odbcview )
{
	SWORD i, nCols;
	ODBCDatabaseVariable *odbcvar;
	UCHAR szColName[ 256 ];
	SWORD cbColName;
	SWORD fSqlType;
	UDWORD ibPrecision;
	SWORD ibScale;
	SWORD fNullable;

	/*
	 * Setup "special" variables (recno, eof, deleted)
	 */

	odbcview->recno		= mvProgram_Allocate( NULL, sizeof( ODBCDatabaseVariable ) ); memset( odbcview->recno,		0, sizeof( ODBCDatabaseVariable ) );
	odbcview->eof		= mvProgram_Allocate( NULL, sizeof( ODBCDatabaseVariable ) ); memset( odbcview->eof,		0, sizeof( ODBCDatabaseVariable ) );
	odbcview->deleted	= mvProgram_Allocate( NULL, sizeof( ODBCDatabaseVariable ) ); memset( odbcview->deleted,	0, sizeof( ODBCDatabaseVariable ) );
	
	mvDatabaseView_AddVariable( view, "recno",		5, odbcview->recno );		odbcview->recno->type	= ODBC_INTEGER;
	mvDatabaseView_AddVariable( view, "eof",		3, odbcview->eof );			odbcview->eof->type		= ODBC_INTEGER;
	mvDatabaseView_AddVariable( view, "deleted",	7, odbcview->deleted );		odbcview->deleted->type	= ODBC_INTEGER;

	/*
	 * Bind the remainder of the results
	 */

	if ( SQLNumResultCols( odbcview->hSTMT, &nCols ) != SQL_SUCCESS )								return odbc_error( odbcview->db, "SQLNumResultCols: ", odbcview->hSTMT, SQL_HANDLE_STMT );

	for ( i = 1; i <= nCols; i++ )
	{
		odbcvar			= ( ODBCDatabaseVariable * ) mvProgram_Allocate( NULL, sizeof( ODBCDatabaseVariable ) );
		memset( odbcvar, 0, sizeof( ODBCDatabaseVariable ) );
		odbcvar->column	= i;

		if ( SQLDescribeCol( odbcview->hSTMT, i, szColName, sizeof( szColName ), &cbColName,
							 &fSqlType, &ibPrecision, &ibScale, &fNullable ) != SQL_SUCCESS )	return odbc_error( odbcview->db, "SQLDescribeCol: ", odbcview->hSTMT, SQL_HANDLE_STMT );

		odbc_log( odbcview->db, "--- Result %d: name = '%.*s', sqltype = %d, precision = %d, scale = %d, nullable = %d\n",
				  i,
				  cbColName > 100 ? 100 : cbColName, szColName,
				  fSqlType,
				  ibPrecision,
				  ibScale,
				  fNullable );

		switch ( fSqlType )
		{
			case SQL_BIGINT :
			case SQL_TINYINT :
			case SQL_SMALLINT :
			case SQL_INTEGER :
			case SQL_BIT :
			{
				odbcvar->type	= ODBC_INTEGER;
				if ( SQLBindCol( odbcview->hSTMT, odbcvar->column, SQL_C_SLONG, &( odbcvar->data_integer ), sizeof( odbcvar->data_integer ), &( odbcvar->cbData  ) ) != SQL_SUCCESS )
				{
					return odbc_error( odbcview->db, "SQLBindCol: ", odbcview->hSTMT, SQL_HANDLE_STMT );
				}

				break;
			}
			case SQL_NUMERIC :
			case SQL_DECIMAL :
			case SQL_REAL :
			case SQL_FLOAT :
			case SQL_DOUBLE :
			{
				odbcvar->type	= ODBC_DOUBLE;
				if ( SQLBindCol( odbcview->hSTMT, odbcvar->column, SQL_C_DOUBLE, &( odbcvar->data_double ), sizeof( odbcvar->data_double ), &( odbcvar->cbData  ) ) != SQL_SUCCESS )
				{
					return odbc_error( odbcview->db, "SQLBindCol: ", odbcview->hSTMT, SQL_HANDLE_STMT );
				}

				break;
			}
			case SQL_LONGVARBINARY :
			case SQL_LONGVARCHAR :
			{
				odbcvar->type			= ODBC_BLOB;
				odbcvar->data_blob_stmt	= odbcview->hSTMT;
				odbcvar->data_blob_col	= i;

				break;
			}
			case SQL_CHAR :
			default :
			{
				odbcvar->type	= ODBC_STRING;

				if ( !ibPrecision && !ibScale )		odbcvar->data_string_size	= 50;
				else								odbcvar->data_string_size	= ibPrecision + ibScale + 1;

				odbcvar->data_string	= ( char * ) mvProgram_Allocate( NULL, odbcvar->data_string_size + 1 );
				if ( SQLBindCol( odbcview->hSTMT, odbcvar->column, SQL_C_CHAR, odbcvar->data_string, odbcvar->data_string_size, &( odbcvar->cbData  ) ) != SQL_SUCCESS )
				{
					return odbc_error( odbcview->db, "SQLBindCol: ", odbcview->hSTMT, SQL_HANDLE_STMT );
				}
				
				break;
			}
		}

		mvDatabaseView_AddVariable( view, szColName, cbColName, odbcvar );
	}

	return 1;
}

/*
 * odbc_load_row
 */

int odbc_load_row( ODBCDatabaseView *view, int row )
{
	UDWORD cRow;
	UWORD rgfStatus;

	if ( view->forwardonly )
	{
		while ( ( view->eof->data_integer == 0 ) && ( view->recno->data_integer < row ) )
		{
			view->recno->data_integer++;

			switch ( SQLExtendedFetch( view->hSTMT, SQL_FETCH_NEXT, row - view->recno->data_integer, &cRow, &rgfStatus ) ) 
			{
				case SQL_ERROR			: return odbc_error( view->db, "SQLExtendedFetch: ", view->hSTMT, SQL_HANDLE_STMT );
				case SQL_NO_DATA_FOUND	: view->eof->data_integer = 1;	break;
			}
		}
	}
	else
	{
		switch ( SQLExtendedFetch( view->hSTMT, SQL_FETCH_ABSOLUTE, row, &cRow, &rgfStatus ) ) 
		{
			case SQL_ERROR			: return odbc_error( view->db, "SQLExtendedFetch: ", view->hSTMT, SQL_HANDLE_STMT );
			case SQL_NO_DATA_FOUND	: view->eof->data_integer = 1;		break;
			default					: view->recno->data_integer = row;	break;
		}
	}

	view->deleted->data_integer	= ( rgfStatus == SQL_ROW_DELETED ) ? 1 : 0;

	odbc_log( view->db, "*** odbc_load_row( %d ), eof = %d, deleted = %d\n",
			  row,
			  view->eof->data_integer,
			  view->deleted->data_integer );

	return 1;
}

/*
 * odbc_db_open
 */

int	odbc_db_open( mvDatabase db,
				  const char *path,		int path_length,
				  const char *name,		int name_path,
				  const char *user,		int user_length, 
				  const char *password,	int password_length ,
				  const char *flags,	int flags_length )
{
	int i, driverconnect;
	ODBCDatabase *dbcontext;
	UCHAR szConnStrOut[ 255 ];
	SWORD cbConnStrOut;

	dbcontext = ( ODBCDatabase *) mvProgram_Allocate( NULL, sizeof( ODBCDatabase ) );
	memset( dbcontext, 0, sizeof( ODBCDatabase ) );
	mvDatabase_SetData( db, dbcontext );

	if ( SQLAllocEnv( &( dbcontext->hEnv ) ) == SQL_ERROR )													return odbc_error( dbcontext, "SQLAllocEnv: ", NULL, 0 );
	if ( SQLAllocConnect( dbcontext->hEnv, &( dbcontext->hDBC ) ) == SQL_ERROR )							return odbc_error( dbcontext, "SQLAllocConnect: ", dbcontext->hEnv, SQL_HANDLE_ENV );
	if ( SQLSetConnectAttr( dbcontext->hDBC, SQL_ATTR_AUTOCOMMIT, SQL_AUTOCOMMIT_OFF, 0 ) != SQL_SUCCESS )	return odbc_error( dbcontext, "SQLSetConnectAttr: ", dbcontext->hDBC, SQL_HANDLE_DBC );

	dbcontext->autocommit	= 1;
	driverconnect			= 0;

	for ( i = 0; i < path_length; i++ )
	{
		if ( path[ i ] == '=' )
		{
			driverconnect = 1;
			break;
		}
	}

	if ( driverconnect )
	{
		cbConnStrOut = sizeof( szConnStrOut );
		if ( SQLDriverConnect( dbcontext->hDBC, NULL, ( UCHAR * ) path, ( SWORD ) path_length, 
							   szConnStrOut, sizeof( szConnStrOut ), &cbConnStrOut, 
							   SQL_DRIVER_NOPROMPT ) == SQL_ERROR )
		{
			odbc_error( dbcontext, "SQLDriverConnect: ", dbcontext->hDBC, SQL_HANDLE_DBC );
			goto error;
		}
	}
	else
	{
		if ( SQLConnect( dbcontext->hDBC, ( UCHAR * ) path, ( SWORD ) path_length,
						 ( UCHAR * ) user, ( SWORD ) user_length,
						 ( UCHAR * ) password, ( SWORD ) password_length ) == SQL_ERROR )
		{
			odbc_error( dbcontext, "SQLConnect: ", dbcontext->hDBC, SQL_HANDLE_DBC );
			goto error;
		}
	}

	return 1;

error:
	return 0;
}

/*
 * odbc_db_close
 */

int	odbc_db_close( mvDatabase db )
{
	ODBCDatabase *dbcontext;

	dbcontext = ( ODBCDatabase * ) mvDatabase_data( db );
	
	if ( dbcontext->hDBC )
	{
		SQLDisconnect( dbcontext->hDBC );
		SQLFreeConnect( dbcontext->hDBC );
	}

	if ( dbcontext->hEnv )
	{
		SQLFreeEnv( dbcontext->hEnv );
	}

	if ( dbcontext->log )
	{
		mvFile_Close( dbcontext->log );
	}

	mvProgram_Free( NULL, dbcontext );
	return 1;
}

/*
 * odbc_db_openview
 */

int odbc_db_openview( mvDatabase db, const char *name, int name_length, const char *query, int query_length, mvVariableList list, int entries )
{
	ODBCDatabase *dbcontext;
	ODBCDatabaseView *viewcontext;
	mvDatabaseView view;
	UCHAR szSqlState[ 50 ];
	SDWORD pfNativeError;
	UCHAR szErrorMessage[ 1024 ];
	SWORD cbErrorMessage;

	dbcontext					= ( ODBCDatabase * ) mvDatabase_data( db );
	viewcontext					= ( ODBCDatabaseView * ) mvProgram_Allocate( NULL, sizeof( ODBCDatabaseView ) );

	memset( viewcontext, 0, sizeof( ODBCDatabaseView ) );

	viewcontext->db				= dbcontext;
	viewcontext->forwardonly	= dbcontext->forwardonly;

	odbc_log( dbcontext, "*** MvOPENVIEW\n" );
	odbc_log_data( dbcontext, query, query_length );

	if ( SQLAllocStmt( dbcontext->hDBC, &( viewcontext->hSTMT ) ) != SQL_SUCCESS )
	{
		odbc_error( dbcontext, "SQLAllocStmt: ", dbcontext->hDBC, SQL_HANDLE_DBC );
		goto error;
	}

	/* 
	 * Some versions of the Oracle ODBC driver require us to make this call in order to return BLOB data correctly,
	 * even if we are setting forwardonly to 1 above.
	 */

	switch ( SQLSetStmtOption( viewcontext->hSTMT, SQL_CURSOR_TYPE, SQL_CURSOR_STATIC ) )
	{
		case SQL_SUCCESS_WITH_INFO :
		{
			if ( SQLError( dbcontext->hEnv, dbcontext->hDBC, viewcontext->hSTMT, szSqlState, &pfNativeError,
						   szErrorMessage, sizeof( szErrorMessage ), &cbErrorMessage ) == SQL_SUCCESS )
			{
				if ( strcmp( ( const char * ) szSqlState, "IM001" ) )
				{
					odbc_error( dbcontext, "SQLSetStmtOption: ", viewcontext->hSTMT, SQL_HANDLE_STMT );
					goto error;
				}
			}

			/* Fall through */
		}
		case SQL_ERROR :
		{
			viewcontext->forwardonly = 1;
			break;
		}
		default :
		{
			break;
		}
	}

	if ( SQLSetStmtOption( viewcontext->hSTMT, SQL_ROWSET_SIZE, 1 ) == SQL_ERROR )
	{
		odbc_error( dbcontext, "SQLSetStmtOption: ", viewcontext->hSTMT, SQL_HANDLE_STMT );
		goto error;
	}

	if ( SQLPrepare( viewcontext->hSTMT, ( char * ) query, query_length ) == SQL_ERROR )
	{
		odbc_error( dbcontext, "SQLPrepare: ", viewcontext->hSTMT, SQL_HANDLE_STMT );
		goto error;
	}

	if ( !odbc_execute( dbcontext, viewcontext->hSTMT, list ) )	goto error;

	view	= mvDatabase_AddView( db, name, name_length, viewcontext );

	if ( !odbc_bind_columns( view, viewcontext ) )				goto error;
	if ( !odbc_load_row( viewcontext, 1 ) )						goto error;

	return 1;

error:
	return 0;
}

/*
 * odbc_db_runquery
 */

int odbc_db_runquery( mvDatabase db, const char *query, int query_length, mvVariableList list, int entries )
{
	SQLHSTMT hSTMT;
	ODBCDatabase *dbcontext;

	hSTMT		= SQL_NULL_HSTMT;
	dbcontext	= ( ODBCDatabase * ) mvDatabase_data( db );

	odbc_log( dbcontext, "*** MvQUERY\n" );
	odbc_log_data( dbcontext, query, query_length );

	if ( SQLAllocStmt( dbcontext->hDBC, &hSTMT ) == SQL_ERROR )
	{
		odbc_error( dbcontext, "SQLAllocStmt: ", dbcontext->hDBC, SQL_HANDLE_DBC );
		goto error;
	}

	if ( SQLPrepare( hSTMT, ( char * ) query, query_length ) == SQL_ERROR )
	{
		odbc_error( dbcontext, "SQLPrepare: ", hSTMT, SQL_HANDLE_STMT );
		goto error;
	}

	if ( !odbc_execute( dbcontext, hSTMT, list ) )	goto error;

	if ( dbcontext->autocommit && !dbcontext->in_transaction )
	{
		SQLEndTran( SQL_HANDLE_DBC, dbcontext->hDBC, SQL_COMMIT );
	}

	SQLFreeStmt( hSTMT, SQL_DROP );
	return 1;

error:

	if ( hSTMT != SQL_NULL_HSTMT )	SQLFreeStmt( hSTMT, SQL_DROP );
	return 0;
}

/*
 * odbc_db_error
 */

const char *odbc_db_error( mvDatabase db )
{
	ODBCDatabase *dbcontext;

	dbcontext = ( ODBCDatabase * ) mvDatabase_data( db );
	return dbcontext->error;
}

/*
 * odbc_dbview_close
 */

int	odbc_dbview_close( mvDatabaseView dbview )
{
	ODBCDatabaseView *viewcontext;

	viewcontext = ( ODBCDatabaseView * ) mvDatabaseView_data( dbview );
	
	if ( viewcontext->hSTMT )	SQLFreeStmt( viewcontext->hSTMT, SQL_DROP );
	mvProgram_Free( NULL, viewcontext );

	return 1;
}

/*
 * odbc_dbview_skip
 */

int odbc_dbview_skip( mvDatabaseView dbview, int rows )
{
	int ok;
	ODBCDatabaseView *viewcontext;

	viewcontext = ( ODBCDatabaseView * ) mvDatabaseView_data( dbview );

	ok = odbc_load_row( viewcontext, viewcontext->recno->data_integer + rows );
	mvDatabaseView_SetDirty( dbview );

	return ok;
}

/*
 * odbc_dbview_go
 */

int odbc_dbview_go( mvDatabaseView dbview, int row )
{
	int ok;
	ODBCDatabaseView *viewcontext;

	viewcontext = ( ODBCDatabaseView * ) mvDatabaseView_data( dbview );

	ok = odbc_load_row( viewcontext, row );
	mvDatabaseView_SetDirty( dbview );

	return ok;
}

/*
 * odbc_dbview_revealstructureagg
 */

int odbc_dbview_revealstructureagg( mvDatabaseView dbview, mvVariable **array )
{
	SQLSMALLINT i, numcols;
	char szColName[ 128 ];
	SQLSMALLINT cbColName, fSQLType, ibScale, fNullable;
	SQLUINTEGER cbColDef;
	ODBCDatabase *dbcontext;
	ODBCDatabaseView *viewcontext;
	mvVariable var_entry, var_name, var_type, var_len, var_dec;

	viewcontext = ( ODBCDatabaseView * ) mvDatabaseView_data( dbview );
	dbcontext	= ( ODBCDatabase * ) mvDatabase_data( mvDatabaseView_Database( dbview ) );

	if ( SQLNumResultCols( viewcontext->hSTMT, &numcols ) != SQL_SUCCESS )	return odbc_error( dbcontext, "SQLNumResultCols: ", viewcontext->hSTMT, SQL_HANDLE_STMT );
	
	for ( i = 1; i <= numcols; i++ )
	{
		if ( SQLDescribeCol( viewcontext->hSTMT, i, szColName, sizeof( szColName ), &cbColName, &fSQLType, &cbColDef, &ibScale, &fNullable ) == SQL_ERROR )
		{
			return odbc_error( dbcontext, "SQLDescribeCol: ", viewcontext->hSTMT, SQL_HANDLE_STMT );
		}

		var_entry	= mvVariable_Array_Element( i, *array, 1 );
		var_name	= mvVariable_Struct_Member( "FIELD_NAME",	10,	var_entry, 1 );
		var_type	= mvVariable_Struct_Member( "FIELD_TYPE",	10,	var_entry, 1 );
		var_len		= mvVariable_Struct_Member( "FIELD_LEN",	9,	var_entry, 1 );
		var_dec		= mvVariable_Struct_Member( "FIELD_DEC",	9,	var_entry, 1 );

		mvVariable_SetValue( var_name, szColName, cbColName );

		switch ( fSQLType )
		{
			case SQL_DECIMAL		:
			case SQL_NUMERIC		:
			case SQL_SMALLINT		:
			case SQL_INTEGER		:
			case SQL_REAL			:
			case SQL_FLOAT			:
			case SQL_DOUBLE			:
			case SQL_TINYINT		:
			case SQL_BIGINT			: mvVariable_SetValue( var_type, "N", 1 );	break;

			case SQL_BIT			: mvVariable_SetValue( var_type, "B", 1 );	break;

			case SQL_LONGVARCHAR	:
			case SQL_LONGVARBINARY	: mvVariable_SetValue( var_type, "M", 1 );	break;

			case SQL_CHAR			:
			case SQL_VARCHAR		:
			default					: mvVariable_SetValue( var_type, "C", 1 );	break;
		}

		mvVariable_SetValue_Integer( var_len, cbColDef );
		mvVariable_SetValue_Integer( var_dec, ibScale );
	}

	return 1;
}

/*
 * odbc_dbview_error
 */

const char *odbc_dbview_error( mvDatabaseView dbview )
{
	return odbc_db_error( mvDatabaseView_Database( dbview ) );
}

/*
 * odbc_dbvar_getvalue_int
 */
	
int odbc_dbvar_getvalue_int( mvDatabaseVariable dbvar, int *value )
{
	ODBCDatabaseVariable *var;

	var = ( ODBCDatabaseVariable * ) mvDatabaseVariable_data( dbvar );
	if ( var->type == ODBC_INTEGER )
	{
		if ( var->cbData == SQL_NULL_DATA || var->cbData == SQL_NO_DATA )	return 0;	// All NULL values go through dbvar_getvalue_string
		else																*value	= var->data_integer;

		return 1;
	}

	return 0;
}
	
/*
 * odbc_dbvar_getvalue_double
 */
	
int odbc_dbvar_getvalue_double( mvDatabaseVariable dbvar, double *value )
{
	ODBCDatabaseVariable *var;

	var = ( ODBCDatabaseVariable * ) mvDatabaseVariable_data( dbvar );
	if ( var->type == ODBC_DOUBLE )
	{
		if ( var->cbData == SQL_NULL_DATA || var->cbData == SQL_NO_DATA )	return 0;	// All NULL values go through dbvar_getvalue_string
		else																*value	= var->data_double;

		return 1;
	}

	return 0;
}

/*
 * odbc_dbvar_getvalue_string
 */

int odbc_dbvar_getvalue_string( mvDatabaseVariable dbvar, char **value, int *value_length, int *value_del )
{
	SQLRETURN result;
	SQLINTEGER blob_len;
	char *buffer, *temp_buffer;
	int buffer_size;
	ODBCDatabaseVariable *var;

	var = ( ODBCDatabaseVariable * ) mvDatabaseVariable_data( dbvar );
	if ( var->cbData == SQL_NULL_DATA )
	{
		*value			= "";
		*value_length	= 0;
		*value_del		= 0;

		return 1;
	}
	else if ( var->type == ODBC_STRING )
	{
		*value			= var->data_string;
		*value_length	= var->cbData;
		*value_del		= 0;

		return 1;
	}
	else if ( var->type == ODBC_BLOB )
	{
		buffer_size		= 512;
		buffer			= ( char * ) mvProgram_Allocate( NULL, buffer_size + 1 + 1 ); /* The extra byte is required because an Oracle developer can't count */

		result			= SQLGetData( var->data_blob_stmt, var->data_blob_col, SQL_C_CHAR, buffer, buffer_size + 1, &blob_len );

		if ( result == SQL_ERROR )
		{
			/* Call odbc_error for logging */
			odbc_error( ( ODBCDatabase * ) mvDatabase_data( mvDatabaseView_Database( mvDatabaseVariable_DatabaseView( dbvar ) ) ),
				        "SQLGetData: ", var->data_blob_stmt, SQL_HANDLE_STMT );

			mvProgram_Free( NULL, buffer );

			*value			= "";
			*value_length	= 0;
			*value_del		= 0;

			return 1;
		}
		else if ( result == SQL_NO_DATA || blob_len == SQL_NULL_DATA || blob_len == SQL_NO_TOTAL )
		{
			mvProgram_Free( NULL, buffer );

			*value			= "";
			*value_length	= 0;
			*value_del		= 0;

			return 1;
		}
		else if ( result == SQL_SUCCESS )
		{
			*value			= buffer;
			*value_length	= blob_len;
			*value_del		= 1;
		}
		else if ( result == SQL_SUCCESS_WITH_INFO )
		{
			if ( blob_len == SQL_NO_TOTAL )
			{
				/* Eventually we may want to modify this code to loop until we run out of data */

				*value			= buffer;
				*value_length	= MIVA_LENGTH_ASCIZ;
				*value_del		= 1;
			}
			else
			{
				temp_buffer		= mvProgram_Allocate( NULL, blob_len + 1 + 1 ); /* The extra byte is required because an Oracle developer can't count */

				memcpy( temp_buffer, buffer, buffer_size );
				mvProgram_Free( NULL, buffer );

				buffer			= temp_buffer;

				if ( SQLGetData( var->data_blob_stmt, var->data_blob_col, SQL_C_CHAR, &buffer[ buffer_size ], blob_len - buffer_size + 1, NULL ) != SQL_SUCCESS )
				{
					mvProgram_Free( NULL, buffer );

					*value			= "";
					*value_length	= 0;
					*value_del		= 0;

					return 1;				
				}

				*value			= buffer;
				*value_length	= blob_len;
				*value_del		= 1;
			}
		}

		odbc_log( ( ODBCDatabase * ) mvDatabase_data( mvDatabaseView_Database( mvDatabaseVariable_DatabaseView( dbvar ) ) ),
				  "+++ BLOB data for column %d: length = %d, data = '%.*s'\n",
				  var->column,
				  *value_length,
				  *value_length < 4096 ? *value_length : 4096,
				  *value );

		return 1;
	}

	return 0;
}

/*
 * odbc_dbvar_cleanup
 */

void odbc_dbvar_cleanup( mvDatabaseVariable dbvar )
{
	ODBCDatabaseVariable *var;

	var = ( ODBCDatabaseVariable * ) mvDatabaseVariable_data( dbvar );

	if ( var->data_string )	mvProgram_Free( NULL, var->data_string );
	mvProgram_Free( NULL, var );
}

/*
 * odbc_dbvar_preferred_type
 */

int odbc_dbvar_preferred_type( mvDatabaseVariable dbvar )
{
	ODBCDatabaseVariable *var;

	var = ( ODBCDatabaseVariable * ) mvDatabaseVariable_data( dbvar );

	if ( var->cbData == SQL_NULL_DATA || var->cbData == SQL_NO_DATA )
	{
		return MVD_TYPE_STRING;
	}
	
	switch ( var->type )
	{
		case ODBC_INTEGER	: return MVD_TYPE_INTEGER;
		case ODBC_DOUBLE	: return MVD_TYPE_DOUBLE;
		case ODBC_STRING	:
		case ODBC_BLOB		: return MVD_TYPE_STRING;
	}

	return MVD_TYPE_NONE;
}

/*
 * odbc_db_commit
 */

int odbc_db_commit( mvDatabase db )
{
	ODBCDatabase *dbcontext;

	dbcontext					= ( ODBCDatabase * ) mvDatabase_data( db );
	if ( SQLEndTran( SQL_HANDLE_DBC, dbcontext->hDBC, SQL_COMMIT ) == SQL_ERROR )	return odbc_error( dbcontext, "SQLEndTran: ", dbcontext->hDBC, SQL_HANDLE_DBC );
	dbcontext->in_transaction	= 0;

	return 1;
}

/*
 * odbc_db_rollback
 */

int odbc_db_rollback( mvDatabase db )
{
	ODBCDatabase *dbcontext;

	dbcontext					= ( ODBCDatabase * ) mvDatabase_data( db );
	if ( SQLEndTran( SQL_HANDLE_DBC, dbcontext->hDBC, SQL_ROLLBACK ) == SQL_ERROR )	return odbc_error( dbcontext, "SQLEndTran: ", dbcontext->hDBC, SQL_HANDLE_DBC );
	dbcontext->in_transaction	= 0;

	return 1;
}

/*
 * odbc_db_transact
 */

int odbc_db_transact( mvDatabase db )
{
	ODBCDatabase *dbcontext;

	dbcontext					= ( ODBCDatabase * ) mvDatabase_data( db );
	dbcontext->in_transaction	= 1;

	return 1;
}

/*
 * odbc_db_command
 */

int odbc_db_command( mvDatabase db, const char *command, int command_length, const char *parameter, int parameter_length )
{
	ODBCDatabase *dbcontext;

	dbcontext					= ( ODBCDatabase * ) mvDatabase_data( db );

	if ( command_length == 3 && !memcmp( command, "log", 3 ) )
	{
		if ( parameter_length == 0 )
		{
			parameter			= "sql.log";
			parameter_length	= 7;
		}

		if ( dbcontext->log )
		{
			mvFile_Close( dbcontext->log );
			dbcontext->log = NULL;
		}

		if ( ( dbcontext->log = mvFile_Open( mvDatabase_Program( db ), MVF_DATA, parameter, parameter_length, MVF_MODE_CREATE | MVF_MODE_APPEND | MVF_MODE_WRITE ) ) == NULL )
		{
			strcpy( dbcontext->error, "Unable to open logfile" );
			return 0;
		}
	}
	else if ( command_length == 12 && !memcmp( command, "manualcommit", 12 ) )		dbcontext->autocommit	= 0;
	else if ( command_length == 10 && !memcmp( command, "autocommit", 10 ) )		dbcontext->autocommit	= 1;
	else if ( command_length == 8 && !memcmp( command, "truncate", 8 ) )			dbcontext->truncate		= 1;
	else if ( command_length == 11 && !memcmp( command, "forwardonly", 11 ) )		dbcontext->forwardonly	= 1;
	
	return 1;
}

/*
 * miva_function_table
 *
 * Defines the functions that Miva scripts can call from this shared object
 */

MV_EL_Database *miva_database_library()
{
	static MV_EL_Database miva_dblib = 
	{	MV_EL_DATABASE_VERSION,
		0,
		odbc_db_open, 
		odbc_db_close, 
		odbc_db_openview,
		odbc_db_runquery,

		0,
		0,
		0,
		0,
		0,

		odbc_db_error,

		odbc_dbview_close,
		odbc_dbview_skip,
		odbc_dbview_go,
		0,
		0,
		0,
		0,
		0,
		0,
		0,
		odbc_dbview_revealstructureagg,
		odbc_dbview_error,

		odbc_dbvar_getvalue_int,
		odbc_dbvar_getvalue_double,
		odbc_dbvar_getvalue_string,

		0,
		0,
		0,

		odbc_dbvar_cleanup,
		
		odbc_db_commit,
		odbc_db_rollback,

		odbc_dbvar_preferred_type,

		odbc_db_transact,
		odbc_db_command
	};

	return &miva_dblib;
}


