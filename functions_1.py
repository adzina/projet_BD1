import sqlite3
import df
#declaration of global variables
connection=None #connection to the database
all_dfs=[]  #array of objects of type df, stores all functional dependencies

def init():
	"""Opens the connection to the database and reads all the DFs from the FuncDep table
		If the FuncDep table is not found, it creates it
	"""
	global connection
	
	#creates a connection with database named database.db
	connection = sqlite3.connect('database.db')

	cursor = connection.cursor()
	try:
		cursor.execute("SELECT * FROM FuncDep")
	except sqlite3.OperationalError:
		cursor.execute("""CREATE TABLE FuncDep(
							table_name text,
							lhs text,
							rhs text
			)""")
	#commits changes	
	connection.commit()	
	raw_data=cursor.fetchall()
	print(raw_data)
	for i in range (len(raw_data)):
		table_name=raw_data[i][0]
		lhs=convert_lhs_to_array(raw_data[i][1])
		rhs=raw_data[i][2]
		all_dfs.append(df.df(table_name,lhs,rhs))
	
	
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
		all_dfs.append(new_df)
		print("===============================================\nYou added the following functional dependency:")
		print(new_df.print_me())
		#adding df to database
		cursor = connection.cursor()
		str = "INSERT INTO FuncDep VALUES('"+table_name+"','"+lhs_tmp+"','"+rhs+"')"
		cursor.execute(str)	
		
def close():	
	"""Commits all the changes and closes the connection to the database.
	"""
	connection.commit()
	connection.close()
			

=======
import sqlite3
import df
#declaration of global variables
connection=None #connection to the database
all_dfs=[]  #array of objects of type df, stores all functional dependencies

def init():
	"""Opens the connection to the database and reads all the DFs from the FuncDep table
		If the FuncDep table is not found, it creates it
	"""
	global connection
	
	#user have to enter the name of the database
	database = input("Enter the name of the database : ")
	#creates a connection with database named database.db
	connection = sqlite3.connect(database + '.db')

	cursor = connection.cursor()
	try:
		cursor.execute("SELECT * FROM FuncDep")
	except sqlite3.OperationalError:
		cursor.execute("""CREATE TABLE FuncDep(
							table_name text,
							lhs text,
							rhs text
			)""")
	#commits changes	
	connection.commit()	
	raw_data=cursor.fetchall()
	print(raw_data)
	for i in range (len(raw_data)):
		table_name=raw_data[i][0]
		lhs=convert_lhs_to_array(raw_data[i][1])
		rhs=raw_data[i][2]
		all_dfs.append(df.df(table_name,lhs,rhs))
	#run the application until user want to quit it
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
		"""Allows the user to add a new DF thso the database
		"""
		print("adding a new functional dependency: table_name lhs->rhs\n")
		table_name=input("enter name of the table: ")
		lhs_tmp=input("enter attributes on the left hand side separated by space: ")
		lhs=convert_lhs_to_array(lhs_tmp)
		rhs=input("enter one attribute on the right hand side: ")
		new_df=df.df(table_name,lhs,rhs)
		#adding df to local storage
		all_dfs.append(new_df)
		print("===============================================\nYou added the following functional dependency:")
		print(new_df.print_me())
		#adding df to database
		cursor = connection.cursor()
		str = "INSERT INTO FuncDep VALUES('"+table_name+"','"+lhs_tmp+"','"+rhs+"')"
		cursor.execute(str)	
		
def delete_DF():
		"""Allows the user to delete a DF from the database
		"""
		print("delete a functional dependency\n")
		table_name=input("enter name of the table : ")
		lhs_tmp=input("enter attributes on the left hand side separated by space: ")
		lhs=convert_lhs_to_array(lhs_tmp)
		rhs=input("enter one attribute on the right hand side: ")
		if isInDFList(df.df(table_name,lhs,rhs)) == True:
			cursor = connection.cursor()
			cursor.execute('''DELETE FROM FuncDep WHERE table_name = ? AND lhs = ? AND rhs = ?''', (table_name, lhs_tmp, rhs,))
		else:
			print("Error : DF not found\n")

def modify_DF(old_df, new_df):
		"""Replace a DF by a new DF
		"""
		if isInDFList(old_df) == True:
			cursor = connection.cursor()
			cursor.execute('''DELETE FROM FuncDep WHERE table_name = ? AND lhs = ? AND rhs = ?''', (old_df.table_name, old_df.lhs, old_df.rhs,))
			cursor.execute('''INSERT INTO FuncDep VALUES ( ? , ? , ?)''', (new_df.table_name, new_df.lhs, new_df.rhs,))
		else:
			print("Error : DF not found, can't replace it\n")

def isInDFList(df):
		"""Check if a DF is in the array of DF
		"""
		inList = False
		for i in range(len(all_dfs)):
			if df.equals(all_dfs[i]) == True:
				inList = True
		return inList

def runApp():
		running = True
		while running:
			command = input("Enter your command : ")
			if command == "Add":
				add_DF()
			elif command == "Delete":
				delete_DF()
			#elif command == "Modify":
			elif command == "Exit":
				running = False
			

def close():	
	"""Commits all the changes and closes the connection to the database.
	"""
	connection.commit()
	connection.close()
			