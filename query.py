import sqlite3

class database():
	

	def __init__(self, db_loc):
		self.con = sqlite3.connect("./" + db_loc)
		self.c = self.con.cursor()
		self.initial_setup()
	
	def __del__(self):
		self.con.commit()
		self.con.close()
	
	def initial_setup(self):
		symb = input("Symbol to test: ")
		sym = self.c.execute("SELECT * FROM history WHERE underlyingsymbol=?", (str(symb),))
		
		runningcb = 0
		runnings = []
		count = 0
		for each in sym:
			if each[2] == "Equity Option" and each[5] == "Buy to Close":
				runningcb -= float(each[20])
			elif each[2] == "Equity Option" and each[5] == "Buy to Open":
				runningcb -= float(each[20])
			elif each[2] == "Equity Option" and each[5] == "Sell to Close":
				runningcb += float(each[20])
			elif each[2] == "Equity Option" and each[5] == "Sell to Open":
				runningcb += float(each[20])
			if each[2] == "Equity" and each[5] == "Buy to Close":
				runnings.append([each[6], each[20]])
			elif each[2] == "Equity" and each[5] == "Buy to Open":
				runnings.append([each[6], each[20]])
			elif each[2] == "Equity" and each[5] == "Sell to Close":
				runnings.append([each[6], each[20]])
			elif each[2] == "Equity" and each[5] == "Sell to Open":
				runnings.append([each[6], each[20]])

		ttl = 0
		for each in runnings:
			ttl += each[0]
		runninga = []
		for each in runnings:
			runninga.append(each[1] / ttl)
		
		ttl = 0
		for each in runninga:
			ttl += each
		print("TTL ", ttl)
		
		runningcb = runningcb/100
		print(runningcb, ttl, ttl - runningcb)
main = database("main.db")
