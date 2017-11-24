import df
import sqlite3
import copy

connection=None

def show_all_DF_not_satisfied(all_dfs, conn):
	global connection
	connection=conn
	not_satisfied=[]
	for i in range(len(all_dfs)):
		if(not verify_DF_satisfied(all_dfs[i])):
			not_satisfied.append(df.df(all_dfs[i].table_name,all_dfs[i].lhs,all_dfs[i].rhs))
			
	
	if(len(not_satisfied)>0):	
		print("The following DFs are not satisfied:")
		for i in range(len(not_satisfied)):
			print(not_satisfied[i].print_me())
	else:
		print("All DFs are satisfied")
def verify_DF_satisfied(df):
	"""Checks if the DF is satisfied. 
	   Goes through all the pairs (lhs,rhs) selected from the given table_name
	   and checks if for the same values of lhs the rhs is always the same.
			input:	
				df: df
			output: 
				satisfied: bool
				
	   Example:
	   df=(table_name="employee", lhs="name lastname" rhs="address")
	   df is satisfied for the following table:
	   employee| name	|	lastname	| address
				Jack	 Sparrow		 London, Picadilly Circus 17
				Ada		 Lovelace		 London, Fann Street 9
		
		but not by the following table:		
		employee| name	|	lastname	| address
				 Jack	 Sparrow		 London, Picadilly Circus 17
				 Ada	 Lovelace		 London, Fann Street 9
				 Ada	 Lovelace		 London, Picadilly Circus 17
		
	
	"""
	global connection
	cursor = connection.cursor()
	#reads all the data from columns present in DF
	str="SELECT "
	for i in range (len(df.lhs)):
		str=str+df.lhs[i]+", "
	str=str+df.rhs+" FROM "+df.table_name
	cursor.execute(str)
	tuples=cursor.fetchall()
	
	#contains associations between lhs and rhs. For one lhs only one rhs is acceptable
	#If for all tuples with the same lhs, rhs remains the same, the DF is met
	assoc=[]
	
	for i in range(len(tuples)):
		val=search_in_array(assoc,tuples[i][:-1])
		if(val==None):
			#if lhs was not found in the association table program adds it
			assoc.append(tuples[i])
		elif(val!=tuples[i][-1]):
			#if lhs was found in association table 
			#and the rhs there is different than the rhs in the tuple that is being processed
			#the DF is not satisfied
			return False	
	return True		
	
	
def search_in_array(array, lhs):
	for i in range (len(array)):
		if(array[i][:-1]==lhs):
			return array[i][-1]
	return None		
	
	
	
	