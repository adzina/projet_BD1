import df
import sqlite3
import config
import itertools
import functions_1
import copy

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
	Delete all the invalid functional dependencies from the funcDep table
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
	
def multi_delete(nrs):
		"""
		Removes multiple DFs from global storage
		:param nrs: indices of elements to be removed
		:return: None
		"""
		indexes = sorted(list(nrs), reverse=True)
		for index in indexes:
			del config.all_dfs[index]	
def convert_lhs_to_string(lhs):
		str=""
		for i in range(len(lhs)):
			str=lhs[i]+" "
		return str
		
def get_all_attributes(table_name):
		"""Find names of all columns in a table
			:param table_name: name of a table
			:return: array of names
		"""
		cursor = config.connection.cursor()
		cursor.execute('SELECT * FROM {}'.format(table_name,))
		#gets names of all columns in df's table
		names = list(map(lambda x: x[0], cursor.description))
		return names

def findsubsets(S,m):
		"""Find subsets of set
			:param S: set
			:param m: number of elements in subset
			:return: all subsets of size m from set S
		"""
		return set(itertools.combinations(S, m))
	
def find_all_super_keys(table_name):
		sk_list=[]
		tmp=set()
		pk=find_primary_key(table_name)
		for i in pk:
			sk=find_super_keys_from_pk(set(i),table_name)
			sk_list.extend(sk)
		sk_list=remove_repetitions(sk_list)	
		return sk_list
def remove_repetitions(table):
		res=[]
		flag=True
		for i in range(len(table)):
			for j in range(i+1,len(table)):
				if(table[j].issubset(table[i]) and table[i].issubset(table[j])):
					flag=False
			if(flag==True):
				res.append(table[i])
			else:
				flag=True
		return res		
def find_super_keys_from_pk(pk,table_name):
		"""
		:param table_name: name of a relation
		:return: set of all super keys based on the primary key of this relation
		"""				
		sk=set()
		sk_list=[]
		attr=get_all_attributes(table_name)
		other_args=set(attr).difference(pk)
		sk_list.append(pk)
		for i in range(len(other_args)):
			subs=findsubsets(other_args,i+1)
			for j in subs:
				tmp=pk.copy()
				tmp=pk|set(j)
				sk.update(tmp)
				sk_list.append(sk)
				sk=set()
		return sk_list	
		
"""
To determine the primary key, the algoritm divises arguments into two categories:
left and middle.

left - attributes that never occur on the rhs of a DF
middle - attribute that can be found in both rhs and lhs 

Algorithm starts with the left set and adds to it only those middle attributes which cannot be defined by argument already in left set
"""		
def sort_into_left_and_middle(attr,df_of_this_table):
	"""
	Sort attributes into two groups: left and middle
	left - attributes that never occur on the rhs of a DF
	middle - attribute that can be found in both rhs and lhs 
	:param attr: list of all attributes of a table
	:param df_of_this_table: list of all dependencies assigned to this table
	:return: a tuple (left,middle)
	"""
	in_left=False
	in_right=False
	left=[]
	middle=[]
	for i in range(len(attr)):
		for j in range(len(df_of_this_table)):
			if(attr[i] in df_of_this_table[j].lhs):
				in_left=True
			if(attr[i] in df_of_this_table[j].rhs):
				in_right=True
		if (in_left and in_right):
			middle.append(attr[i])
		elif(in_left and not in_right):
			left.append(attr[i])
		in_left=False
		in_right=False
	return (left,middle)
'''
def check_middles(left,middle,df_of_this_table):
		flag=True
		for i in range(len(middle)):
			for j in range(len(df_of_this_table)):
				for k in  range(len(df_of_this_table[j].lhs)):
					if(df_of_this_table[j].rhs==middle[i] and df_of_this_table[j].lhs[k] in left):
						flag=False
				
				if(flag==True):	
					left.append(middle[i])
					middle.pop(i)
					(left,middle)=check_middles(left,middle,df_of_this_table)
					return (left,middle)
						
		return(left,middle)
'''		
def check_all_sets(left,middle,attr,df_of_this_table):
		"""Verify all combinations of middle attributes with all left attributes to find those which closure is a full set of attributes
			:param:left
			:param:middle
			:param:attr: all attributes of a table
			:df_of_this_table
			:return:set of candidate keys
		"""
		list_pk=[]
		minimal_pk=[]
		pk=set(left)
		for i in range(len(middle)):
			subs=findsubsets(set(middle),i+1)
			for j in subs:
				candidate=pk.union(j)
				closure=find_closure(candidate,df_of_this_table)
				if(set(attr).issubset(closure)):
					#closure covers all attributes of a table
					list_pk.append(candidate)
				tmp=set()
		flag=True		
		for i in range(len(list_pk)):
			for j in range(i+1,len(list_pk)):
				if(list_pk[j].issubset(list_pk[i])):
					flag=False
			if(flag==True):
				minimal_pk.append(list_pk[i])
			flag=True	
		return minimal_pk	
def find_closure(attr,df_of_this_table):
		"""Find closure determined by a set of attributes
			:param attr: set of attributes
			:param df_of_this_table:
			:return: closure
		"""
		changed=False
		closure=copy.deepcopy(attr)
		for i in range (len(df_of_this_table)):
			if(set(df_of_this_table[i].lhs)).issubset(attr) and df_of_this_table[i].rhs not in closure:
				closure.update(df_of_this_table[i].rhs)
				changed=True
		if(changed==False):	
			return closure
		else:
			closure=find_closure(closure,df_of_this_table)
			return closure
def find_primary_key(table_name):
		"""
		Return candidate keys of a table
		:param table_name: 
		:return: Candidate keys of the table
		"""
		df_of_this_table=[]
		pk=[]
		attr=get_all_attributes(table_name)
		
		for i in range (len(config.all_dfs)):
			if (table_name==config.all_dfs[i].table_name):
				df_of_this_table.append(config.all_dfs[i])
				
		(left,middle)=sort_into_left_and_middle(attr,df_of_this_table)
		
		#check_middles(left,middle,df_of_this_table)
		#pk=left
		if(set(attr).issubset(find_closure(set(left),df_of_this_table))):
			return set(left)
		else:	
			pk=check_all_sets(left,middle,attr,df_of_this_table)
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
	
        	
def convert_lhs_to_string(lhs):
		"""
		Change a list of attributes to a String
		:param lhs: A list of attributes
		:return: A string
		"""
		str=""
		for i in range(len(lhs)):
			str=str+lhs[i]+" "
		return str
def convert_attr_to_string(attr):
		"""
		Change a list of attributes to a String
		:param lhs: A list of attributes
		:return: A string
		"""
		str=""
		for i in range(len(attr)-1):
			str=str+attr[i]+", "
		str=str+attr[-1]	
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
		
		

"""
3NF: 1.iteruj po wszystkich df danej tabeli				done
	 2. sprawdz czy:
		lhs jest super keys
		lub
		rhs jest w ktoryms z candidate key				done
	 3. zapisz df, dla których nie jest to spełnione  	done
	 4. oblicz dekompozycje tabeli
	 5. eksportuj do nowej bazy danych
	 6. zapisz df w FuncDep
"""	
def verify3NF(table):
		"""Verify whether a table is in 3NF
			:param table: name of a table
			:return invalid_dfs: list of dfs violating the rules of 3NF
		"""
		df_of_this_table=functions_1.getDFs(table)
		sk=find_all_super_keys(table)
		pk=find_primary_key(table)
		primary_attr=set()
		invalid_dfs=[]
		for i in pk:
			primary_attr=primary_attr|set(i)
		for i in df_of_this_table:
			if(set(i.lhs) not in sk and i.rhs not in primary_attr): 
				invalid_dfs.append(i)
		return invalid_dfs
		
def decompose(table,invalid_dfs):
		tmp_attr=[]
		attr=get_all_attributes(table)
		for i in invalid_dfs:
			attr.remove(i.rhs)
		cursor=config.connection.cursor()
		s=convert_attr_to_string(attr)
		
		cursor.execute("""CREATE TABLE {} AS SELECT {} FROM {}""".format(table+"BCNF1",s,table))	
		counter=1
		for i in invalid_dfs:
			counter+=1
			tmp_attr=copy.deepcopy(i.lhs)
			tmp_attr.extend(i.rhs)
			s=convert_attr_to_string(tmp_attr)
			cursor.execute("""CREATE TABLE {} AS SELECT {} FROM {}""".format(table+"BCNF"+str(counter),s,table))
		cursor.execute("""DROP TABLE {}""".format(table,))	
		
			
			
			
			
			
			
			