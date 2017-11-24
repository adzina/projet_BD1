class df(object):

	def __init__(self, table_name, lhs, rhs):
		self.table_name = table_name
		self.lhs = lhs
		self.rhs = rhs
		
	def print_me(self):
		return "DF('{}', '{}' -> '{}')".format(self.table_name,self.lhs,self.rhs)
		
	def lhsEquals(self, lhs):
		equals = True
		if(len(self.lhs) != len(lhs)):
			equals = False
			return equals
		else:
			i = 0
			while i < len(self.lhs):
				if lhs[i] != self.lhs[i]:
					equals = False
					i = len(self.lhs)
				i=i+1
			return equals
			
	def equals(self, df):
		if self.table_name == df.table_name and self.lhsEquals(df.lhs) == True and self.rhs == df.rhs:
			return True
		else:
			return False
		