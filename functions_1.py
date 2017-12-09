import sqlite3
import df
import config
import functions_2

def init():
	"""
	Opens the connection to the database and reads all the DFs from the FuncDep table
		If the FuncDep table is not found, it creates it
	:return: None
	"""
	
	#user has to enter the name of the database
	database = input("Enter the name of the database : ")
	#database="database"
	#creates a connection with the database
	config.connection = sqlite3.connect(database + '.db')

	cursor = config.connection.cursor()
	try:
		cursor.execute("SELECT * FROM FuncDep")
	except sqlite3.OperationalError:
		cursor.execute("""CREATE TABLE FuncDep(
							'table_name' text,
							'lhs' text,
							'rhs' text
			)""")
	#commits changes	
	config.connection.commit()	
	raw_data=cursor.fetchall()
	for i in range (len(raw_data)):
		table_name=raw_data[i][0]
		lhs=convert_lhs_to_array(raw_data[i][1])
		rhs=raw_data[i][2]
		config.all_dfs.append(df.df(table_name,lhs,rhs))
	#run the application until user wants to quit it
	runApp()
	
def convert_lhs_to_array(lhs):
		"""
		Converts a lhs saved as string separated by spaces to an array
		:param lhs: A list of attributes
		:return: An array of attributes
		"""
		list=lhs.split(' ')
		return list
		
def add_DF():
		"""
		Allows the user to add a new DF to the database
		:return: None
		"""
		print("adding a new functional dependency: table_name lhs->rhs\n")
		table_name=input("enter name of the table: ")
		lhs_tmp=input("enter attributes on the left hand side separated by space: ")
		lhs=convert_lhs_to_array(lhs_tmp)
		rhs=input("enter one attribute on the right hand side: ")
		new_df=df.df(table_name,lhs,rhs)
		if canAdd(new_df) == True :
			#adding df to local storage
			config.all_dfs.append(new_df)
			print(len(config.all_dfs))
			print("===============================================\nYou added the following functional dependency:")
			print(new_df.print_me())
			#adding df to database
			cursor = config.connection.cursor()
			cursor.execute('''INSERT INTO FuncDep VALUES (? , ? ,?)''', (table_name, lhs_tmp ,rhs))
			config.connection.commit()
		else:
			print("Can't add DF, Attributes are not in table " + new_df.table_name +"\n")
def delete_DF():
		"""
		Allows the user to delete a DF from the database
		:return: None
		"""
		print("delete a functional dependency\n")
		table_name=input("enter name of the table : ")
		lhs_tmp=input("enter attributes on the left hand side separated by space: ")
		lhs=convert_lhs_to_array(lhs_tmp)
		rhs=input("enter one attribute on the right hand side: ")
		if isInDFList(df.df(table_name,lhs,rhs)) == True:
			cursor = config.connection.cursor()
			cursor.execute('''DELETE FROM FuncDep WHERE table_name = ? AND lhs = ? AND rhs = ?''', (table_name, lhs_tmp, rhs))
			config.connection.commit()
			removeFromDFList(df.df(table_name,lhs,rhs))
		else:
			print("Error : DF not found\n")

def modify_DF():
		"""
		Replace a DF by a new DF
		:return: None
		"""
		print("old DF:\n")
		table_name=input("enter name of the table: ")
		lhs_tmp=input("enter attributes on the left hand side separated by space: ")
		lhs=convert_lhs_to_array(lhs_tmp)
		rhs=input("enter one attribute on the right hand side: ")
		old_df=df.df(table_name,lhs,rhs)
		if isInDFList(old_df) == True:
			cursor = config.connection.cursor()
			cursor.execute('''DELETE FROM FuncDep WHERE table_name = ? AND lhs = ? AND rhs = ?''', (table_name, lhs_tmp, rhs))
			removeFromDFList(old_df)
			print("new DF:\n")
			table_name=input("enter name of the table: ")
			lhs_tmp=input("enter attributes on the left hand side separated by space: ")
			lhs=convert_lhs_to_array(lhs_tmp)
			rhs=input("enter one attribute on the right hand side: ")
			new_df=df.df(table_name,lhs,rhs)
			cursor.execute('''INSERT INTO FuncDep VALUES ( ? , ? , ?)''', (table_name, lhs_tmp, rhs))
			#adding df to local storage
			config.all_dfs.append(new_df)
			config.connection.commit()
		else:
			print("Error : DF not found, can't replace it\n")

def isInDFList(df):
		"""
		Check if a DF is in the array of DF
		:param df: A functional dependencie
		:return: True if the df is in the all_dfs list, False if not
		"""
		inList = False
		for i in range(len(config.all_dfs)):
			if df.equals(config.all_dfs[i]) == True:
				inList = True
		return inList
		
def removeFromDFList(df):
		"""
		Remove df from the array of DF all_dfs
		:param df: The functional dependencie to delete
		:return: None
		"""
		for i in range(len(config.all_dfs)):
			if df.equals(config.all_dfs[i]) == True:
				config.all_dfs.pop(i)
				break
def getDFs(table):
	"""
	return all the functionals dependencies of the table
	:param table: A table of the database
	:return: Array of all the DF of table
	"""
	dfs = []
	for df in config.all_dfs:
		if df.table_name == table:
			dfs.append(df)
	return dfs

def getAttributes(table):
	"""
	return all the attributes of the table
	:param table: A table of the database
	:return: List of all the attributes of the table
	"""
	attributes = []
	cursor = config.connection.cursor()
	for att in cursor.execute("PRAGMA table_info("+ table +")"):
		attributes.append(att[1])
	config.connection.commit()
	return attributes

def canAdd(df):
	attributes = getAttributes(df.table_name)
	if functions_2.isIncluded(df.lhs, attributes) == False or functions_2.isIncluded(df.rhs, attributes) == False :
		return False
	else:
		return True

def runApp():
	"""
	Run the application until user wants to quit it
	:return: None
	"""
	running = True
	while running:
		command = input("Enter your command : ")
		if command == "Add":
			add_DF()
		elif command == "Delete":
			delete_DF()
		elif command == "Modify":
			modify_DF()
		elif command == "Show invalid":
			showInvalid()
		elif command == "Delete invalid":
			functions_2.delete_invalid_DFs()
		elif command == "Show keys":
			showKeys()
		elif command == "Show superkeys":
			showSuperkeys()
		elif command == "Show LogicConseq":
			showLogicalConsequence()
		elif command == "isBCNF":
			showBCNF()
		elif command == "is3NF":
			show3NF()
		elif command == "Exit":
			running = False
			
def showInvalid():
	"""
	Show all the invalid functionals dependencies of the funcDep table
	:return: None
	"""
	not_satisfied=[]
	not_satisfied=functions_2.show_all_DF_not_satisfied()
	
	if(len(not_satisfied)>0):	
		print("The following DFs are not satisfied:")
		for i in range(len(not_satisfied)):
			print(not_satisfied[i].print_me())
	else:
		print("All DFs are satisfied")

def showKeys():
	"""
	Show all candidate keys of a table
	:return: None
	"""
	table_name=input("enter name of the table: ")
	print(functions_2.find_primary_key(table_name))

def showSuperkeys():
	"""
	Show all super keys of a table
	:return: None
	"""
	table_name=input("enter name of the table: ")
	print(functions_2.find_all_super_keys(table_name))

def showLogicalConsequence():
	"""
	Show all the logical Consequence from the DF of a table
	:return: None
	"""
	table_name=input("enter name of the table: ")
	for f in functions_2.getLogicalConsequence(getDFs(table_name)):
		print(f.print_me())
		
def showBCNF():
	"""
	Show if a schema is in BCNF
	:return: None
	"""
	table_name=input("enter name of the table: ")
	print(functions_2.verifyBCNF(table_name))
	
def decompose3NF(valid_tables,invalid_tables):
	"""
	Decompose schema
	:return: None
	"""
	database = input("Enter the name of the database you want to export new tables to: ")
	connection = sqlite3.connect(database + '.db')
	
	
	cursor=connection.cursor()
	#create table FuncDep in new database
	cursor.execute("CREATE TABLE FuncDep ('table_name' text, 'lhs' text, 'rhs' text)")
	connection.commit()
	
	for i in valid_tables:
		functions_2.copy_table(i,connection)
	for i in invalid_tables:
		functions_2.decompose3NF(i,connection)
	
	connection.close()
def getAllTables():
	"""
	Get all tables present in a schema
	:return: list of tables' names
	"""
	cursor=config.connection.cursor()
	cursor.execute("select name from sqlite_master where type = 'table' and name!='FuncDep'")	
	tables=cursor.fetchall()
	return tables
def show3NF():
	"""
	Show if a database is in 3NF
	:return: None
	"""
	tables=getAllTables()
	invalid_tables=[]
	valid_tables=[]
	for i in tables:
		
		invalid_dfs=functions_2.verify_3NF(i[0])
		if(len(invalid_dfs)>0):
			invalid_tables.append(i[0])
			print("the following dfs in table {} violate the 3NF".format(i[0],))
			for j in invalid_dfs:
				print(j.print_me())
		else:
			valid_tables.append(i[0])

	if(len(invalid_tables)>0):
		if(len(functions_2.show_all_DF_not_satisfied())==0):
			rep=input("decompose this schema? (y/n): ")
			if(rep=='y'):
				decompose3NF(valid_tables,invalid_tables)
		else:
			printf("To decompose this schema, first remove invalid functional dependancies")
	else:
		print("this schema is in 3NF")
	

def close():	
	"""
	Commits all the changes and closes the connection to the database.
	:return: None
	"""
	config.connection.commit()
	config.connection.close()

