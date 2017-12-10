To run this application type "py main.py" or "python3 main.py".

Enter a name of the database you want to work with. If a database with
that name does not exist, it will be created automatically.

To see a list of all possible commands type "Help" and press enter.
The command are the following:
	"Show tables"
				Shows all the tables present in the database
	"Show DF"
				Shows all Functional Dependancies of a table
	"Add"
				Allows you to add a new Functional Dependancy to FuncDep
	"Delete"
				Allows you to delete a Functional Dependancy from FuncDep
	"Modify"
				Allows you to modify a Functional Dependancy
	"Show invalid"
				Lists all the Functional Dependancies that are not satisfied
	"Delete invalid"
				Allows you to delete any of the invalid dependancies
	"Show LogicConseq"
				Shows all Functional Dependancies being a logical consequence in a table
	"Show superkeys"
				Lists all superkeys of a table
	"Show keys"
				Lists all candidate keys of a table
	"isBCNF"
				Verifies if schema does not violate the rules of BCNF
	"is3NF"
				Verifies if schema does not violate the rules of 3NF. 
				If rules of 3NF are not satisfied but all Functional Dependancies are satisfied,
				you can choose to normalize the schema and export it to another database without
				modyfying the original data.
	
To quit the application type "Exit" and press enter.
All the changes will be saved automatically.	