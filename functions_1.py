import sqlite3
import df
import config
import functions_2
def init():
	"""Opens the connection to the database and reads all the DFs from the FuncDep table
		If the FuncDep table is not found, it creates it
	"""
	
	#user has to enter the name of the database
	database = input("Enter the name of the database : ")
	#creates a connection with the database
	config.connection = sqlite3.connect(database + '.db')

	cursor = config.connection.cursor()
	try:
		cursor.execute("SELECT * FROM FuncDep")
	except sqlite3.OperationalError:
		cursor.execute("""CREATE TABLE FuncDep(
							table_name text,
							lhs text,
							rhs text
			)""")
	#commits changes	
	config.connection.commit()	
	raw_data=cursor.fetchall()
	print(raw_data)
	for i in range (len(raw_data)):
		table_name=raw_data[i][0]
		lhs=convert_lhs_to_array(raw_data[i][1])
		rhs=raw_data[i][2]
		config.all_dfs.append(df.df(table_name,lhs,rhs))
	functions_2.find_super_key(config.all_dfs[0])	
	#run the application until user wants to quit it
	runApp()
	
def convert_lhs_to_array(lhs):
		"""Converts a lhs saved as string separated by spaces to an array
			input:	
				lhs: string
			output: 
				list: string[]
		"""
		list=lhs.split(' ')
		return list
		
def add_DF():
		"""Allows the user to add a new DF to the database
		"""
		print("adding a new functional dependency: table_name lhs->rhs\n")
		table_name=input("enter name of the table: ")
		lhs_tmp=input("enter attributes on the left hand side separated by space: ")
		lhs=convert_lhs_to_array(lhs_tmp)
		rhs=input("enter one attribute on the right hand side: ")
		new_df=df.df(table_name,lhs,rhs)
		#adding df to local storage
		config.all_dfs.append(new_df)
		print(len(config.all_dfs))
		print("===============================================\nYou added the following functional dependency:")
		print(new_df.print_me())
		#adding df to database
		cursor = config.connection.cursor()
		cursor.execute('''INSERT INTO FuncDep VALUES (? , ? ,?)''', (table_name, lhs_tmp ,rhs))
		config.connection.commit()
		
def delete_DF():
		"""Allows the user to delete a DF from the database
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
		"""Replace a DF by a new DF
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
		"""Check if a DF is in the array of DF
		"""
		inList = False
		for i in range(len(config.all_dfs)):
			if df.equals(config.all_dfs[i]) == True:
				inList = True
		return inList
def removeFromDFList(df):
		for i in range(len(config.all_dfs)):
			if df.equals(config.all_dfs[i]) == True:
				config.all_dfs.pop(i)
				break
def runApp():
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
			elif command == "Exit":
				running = False
			elif command == "Show LogicConseq":
				for f in functions_2.getLogicalConsequence(config.all_dfs):
					print(f.print_me())
			
def showInvalid():
	not_satisfied=[]
	not_satisfied=functions_2.show_all_DF_not_satisfied()
	
	if(len(not_satisfied)>0):	
		print("The following DFs are not satisfied:")
		for i in range(len(not_satisfied)):
			print(not_satisfied[i].print_me())
	else:
		print("All DFs are satisfied")
		
def close():	
	"""Commits all the changes and closes the connection to the database.
	"""
	config.connection.commit()
	config.connection.close()
			
