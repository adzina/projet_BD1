class df:

	def __init__(self, table_name, lhs, rhs):
		self.table_name = table_name
		self.lhs = lhs
		self.rhs = rhs
		
	def print_me(self):
		return "DF('{}', '{}' -> '{}')".format(self.table_name,self.lhs,self.rhs)
		