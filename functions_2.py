import df
import sqlite3
import config
import functions_1

connection=None

def show_all_DF_not_satisfied():
	"""
	return a list of all the unsatisfied DF in the all_dfs
	:return: List of all the unsatisfied DF
	"""
	not_satisfied=[]
	for i in range(len(config.all_dfs)):
		if(not verify_DF_satisfied(config.all_dfs[i])):
			not_satisfied.append(df.df(config.all_dfs[i].table_name,config.all_dfs[i].lhs,config.all_dfs[i].rhs))		
	return not_satisfied
def verify_DF_satisfied(df):
	"""
	Checks if a DF is satisfied.
	:param df: A functional dependencie
	:return: True if the df is satisfied, False if not
	"""			
	   #Example:
	   #df=(table_name="employee", lhs="name lastname" rhs="address")
	   #df is satisfied for the following table:
	   #employee| name	|	lastname	| address
		#		Jack	 Sparrow		 London, Picadilly Circus 17
		#		Ada		 Lovelace		 London, Fann Street 9
		#
		#but not by the following table:		
		#employee| name	|	lastname	| address
		#		 Jack	 Sparrow		 London, Picadilly Circus 17
		#		 Ada	 Lovelace		 London, Fann Street 9
		#		 Ada	 Lovelace		 London, Picadilly Circus 17
		
	cursor = config.connection.cursor()
	#reads all the data from columns present in DF
	str="SELECT "
	for i in range (len(df.lhs)):
		str=str+df.lhs[i]+", "
	str=str+df.rhs+" FROM "+df.table_name
	try:
		cursor.execute(str)
		tuples=cursor.fetchall()	
	except sqlite3.OperationalError:
		#returns False if attributes or tables were not found
		return False
	
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
	"""
	Search a list of attributes in an array
	:param array: An array of lhs
	:param lhs: The list of attributes to find
	:return: The lhs if it is in the array, None if not
	"""
	for i in range (len(array)):
		if(array[i][:-1]==lhs):
			return array[i][-1]
	return None		
	
def delete_invalid_DFs():
	"""
	Delete all the invalid functionals dependencies from the funcDep table
	:return: None
	"""
	not_satisfied=[]
	not_satisfied=show_all_DF_not_satisfied()
	
	logical_consequence=[]
	logical_consequence=getLogicalConsequence(config.all_dfs)
	
	not_satisfied.extend(logical_consequence)
	
	indices=[]
	if(len(not_satisfied)>0):
		print("Redundant DFs:")
		for i in range (len(not_satisfied)):
			print("{}. {}".format(i,not_satisfied[i].print_me()))
		nrs=input("Enter numbers of DFs you wish to delete: ")
		nrs=nrs.split(' ')
		for i in range (len(nrs)):
			tmp=int(nrs[i])
			indices.append(tmp)
		cursor = config.connection.cursor()
		for i in range (len(indices)):
			lhs=convert_lhs_to_string(config.all_dfs[indices[i]].lhs)
			cursor.execute('''DELETE FROM FuncDep WHERE table_name = ? AND lhs = ? AND rhs = ?''', (config.all_dfs[indices[i]].table_name, lhs, config.all_dfs[indices[i]].rhs))
			config.connection.commit()
		
		multi_delete(indices)	
	else:
		print("No redundant DFs")

def isLogicalConsequence(attributes, df):
	"""
	Return all the attributes Y involved by the attributes X (X->Y) and that satisfied the set of DF df
	:param attributes: List of attributes
	:param df: A list of functionals dependencies
	:return: All the attributes involved
	"""
	#Algortihm based on http://web.cecs.pdx.edu/~maier/TheoryBook/MAIER/C04.pdf
	oldDep = []
	newDep = attributes[:]
	f = df[:]
	while newDep != oldDep :
		oldDep = newDep
		for depF in f :
			x = depF.lhs
			if isIncluded(x, newDep) :
				if depF.rhs not in newDep:
					newDep.append(depF.rhs)
	if len(newDep)>len(attributes):
		return newDep[len(attributes):]
	else:
		return []

def getLogicalConsequence(all_dfs):
	"""
	Return the DF that are logical Consequence in the group of df given
	:param all_dfs: A list of functionals dependencies
	:return: A list of logical consequence
	"""
	#Algorithm based on http://web.cecs.pdx.edu/~maier/TheoryBook/MAIER/C04.pdf
	logicalConsequence = []
	for df in all_dfs:
		depFunc = all_dfs[:]
		depFunc.remove(df)
		logicConsequence = isLogicalConsequence(df.lhs, depFunc)
		if df.rhs in logicConsequence:
			logicalConsequence.append(df)
	return logicalConsequence
			
def find_super_key(df):
			'''
			Based on this approach: https://stackoverflow.com/questions/5735592/determine-keys-from-functional-dependencies
			Return a super key based on a DF
			:param df: A functional dependencie
			:return: A super key
			'''
			present=[]
			super_key=[]
			present.extend(df.lhs)
			super_key.extend(df.lhs)
			present.extend(df.rhs)
			cursor = config.connection.cursor()
			str="SELECT * FROM "+df.table_name
			cursor.execute(str)
			#gets names of all columns in df's table
			names = list(map(lambda x: x[0], cursor.description))
			for i in range (len(names)):
				if (names[i] not in present):
					super_key.extend(names[i])
			return super_key
def find_primary_key(table_name):
		"""
		Return the primary key of a table
		:param table_name: 
		:return: A primary key of the table
		"""
		df_of_this_table=[]
		for i in range (len(config.all_dfs)):
			if (table_name==all_dfs[i].table_name):
				df_of_this_table.append(all_dfs[i])
		pk=find_super_key(df_of_this_table[0])
		for i in range (1,len(df_of_this_table)):
			#checks if lhs and rhs are subsets of superkey
			if (set(df_of_this_table[i].lhs).issubset(pk) and set(df_of_this_table[i].rhs).issubset(pk)):
				#removes the rhs from super key in order to minimalize it
				pk.remove(df_of_this_table[i].rhs)
				return pk
	
def verifyBCNF(table):
	"""
	Check if the schema of a table is in BCNF
	:param table: A table of the database
	:return: True if the schema is in BCNF, False if not
	"""
	sigma = []
	for f in functions_1.getDFs(table):
		if(f.table_name == table):
			sigma.append(f)
	for df in sigma:
		#for X->A, check if A not included in X
		if isIncluded(df.rhs, df.lhs) == False:
			#check if there is logical consequence for X
			if len(isLogicalConsequence(df.lhs, sigma)) == 0:
				print("This schema is not in BCNF")
				return False
			else:
				#check if all the logical Consequence from X equals the attributes of the table
				if set(isLogicalConsequence(df.lhs, sigma)+df.lhs) != set(functions_1.getAttributes(table)):
					print("This schema is not in BCNF")
					return False
	return True
	
def multi_delete(nrs):
    indexes = sorted(list(nrs), reverse=True)
    for index in indexes:
        del config.all_dfs[index]
        	
def convert_lhs_to_string(lhs):
		"""
		Change a list of attributes to a String
		:param lhs: A list of attributes
		:return: A string
		"""
		str=""
		for i in range(len(lhs)):
			str=lhs[i]+" "
		return str

def isIncluded(array1, array2):
	"""
	Check if an array is in an other
	:param array1: the array included
	:param array2: the main array
	:return: True if the array1 is included in array2
	"""
	if len(array1) > len(array2) :
		return False
	else:
		for i in array1:
			if i not in array2:
				return False
		return True	
